"""
VWAP + 9 EMA Crossover Backtester
Uses tick data CSV files from MarketTick (L1 ticks, semicolon-delimited), same as ORB_Backtest.py.

Python port of vwap_ema9_crossover_strategy.pine:
  - Ticks are aggregated into N-minute bars (default 3m, aligned to the NY clock)
  - EMA(9) of bar closes, VWAP anchored at 09:30 NY (RTH) or 18:00 NY (CME session)
  - LONG  when EMA crosses above VWAP on a closed bar, SHORT when it crosses below
  - Entries are market orders filled at the first tick of the NEXT bar (no look-ahead)
  - Exits: EMA close exit, fixed stop, take-profit (R:R / points / ticks / opposite cross /
    EMA close in profit), trailing stop, break-even — stops and targets are checked tick by tick
  - Filters + daily limits from the Pine script; all optional
  - Positions are flattened at FLATTEN time each day (no overnight holds)

Optional parameter GRID (optimizer) and in-sample / out-of-sample split (OOS_START).

Differences vs the TradingView version, by design:
  - Stops/targets use real tick order instead of TradingView's OHLC path guess.
  - SL/TP levels are based on the actual fill price from the first tick (Pine uses the signal
    close on the fill bar, then re-bases on the next bar).
  - Every day is simulated independently between the data window start and FLATTEN.
    To compare against TradingView, enable the Pine session filter 09:30-16:00 with
    "Flatten Positions Outside Session" and use the RTH VWAP anchor.
"""

import os
import csv
import time
import calendar
import itertools
from array import array
from datetime import datetime, timezone, timedelta
import zoneinfo
from collections import defaultdict
from multiprocessing import Pool, cpu_count

# ============================================================
#  DATA / INSTRUMENT
# ============================================================
TICK_DATA_DIR = os.environ.get("BT_TICK_DIR", "/Volumes/LaCie/Downloads/Future_NQ_T1")
OUTPUT_DIR    = os.environ.get("BT_OUT_DIR", "/Users/ryukki/Documents/coding/Python/Topstep")
TRADES_CSV    = os.path.join(OUTPUT_DIR, "VWAP_EMA9_Trades.csv")
GRID_CSV      = os.path.join(OUTPUT_DIR, "VWAP_EMA9_Grid.csv")

# Separate from the ORB/CRT cache: this one also stores volume (needed for VWAP) and a wider window.
CACHE_DIR = os.path.join(OUTPUT_DIR, "tick_cache_vwap")

# Tick line layout: "<YYYYMMDDHHMMSSffffff UTC>;...;<type>;<price>;<volume>"
# !! Check VOLUME_FIELD against your files — the ORB script never reads volume. !!
TYPE_FIELD   = 2
TRADE_TYPE   = "2"      # type 2 = Last/Trade tick
PRICE_FIELD  = 3
VOLUME_FIELD = 4
FILES_SORTED = True     # tick lines are in time order: read only the needed window (much faster)

# Parallel workers. On a USB spinning drive (LaCie) 2-4 is often FASTER than 8 for the first
# (uncached) run because parallel reads make the disk seek back and forth. Cached runs: use more.
WORKERS = min(cpu_count(), 8)

TICK_SIZE   = 0.25
POINT_VALUE = 20.0      # $ per point per contract: NQ 20, MNQ 2, ES 50, MES 5
COMMISSION_PER_CONTRACT = 0.54   # per side (entry + exit = 2x per round trip)
SLIPPAGE_TICKS   = 1    # adverse ticks added to market fills (entries, EMA exits, EOD) and stop fills
TP_TRADE_THROUGH = True # limit TP fills only if price trades THROUGH it (conservative queue assumption)

START_DATE = None       # e.g. "2024-01-01" (inclusive), None = all files
END_DATE   = None       # e.g. "2025-12-31" (inclusive)
OOS_START  = None       # e.g. "2025-06-01": trades on/after this date are reported as OUT-OF-SAMPLE

WARMUP_START = (8, 0)   # NY time ticks start loading for RTH anchor (EMA warm-up before 09:30)

# ============================================================
#  STRATEGY SETTINGS — mirrors the Pine inputs
# ============================================================
PARAMS = dict(
    # ---- Indicators ----
    TIMEFRAME_MIN = 3,
    EMA_LEN       = 9,
    VWAP_ANCHOR   = "RTH",     # "RTH" = reset 09:30 NY | "SESSION" = reset 18:00 NY prior day (CME)
    VWAP_SOURCE   = "hlc3",    # "hlc3" = TradingView-style bar VWAP | "tick" = true tick-by-tick VWAP
    IGNORE_RESET_CROSS = True, # no signal on the bar where VWAP resets (the "cross" there only comes
                               # from VWAP jumping to the new anchor; False = same as TradingView)

    # ---- Entries ----
    CONFIRM_CLOSE = False,     # signal bar must also close beyond BOTH EMA and VWAP
    DIRECTION     = "both",    # "both" | "long" | "short"

    # ---- Stop-loss ----
    USE_EMA_STOP   = True,     # Method 1: bar closes back through EMA -> exit next bar open
    USE_FIXED_STOP = True,     # Method 2: hard stop
    STOP_MODE      = "points", # "points" | "ticks" | "pct" (% of position value, Pine default 25)
    STOP_VALUE     = 25.0,     # distance in the unit above; also defines 1R

    # ---- Take-profit ----
    TP_MODE   = "none",        # "none" | "rr" | "points" | "ticks" | "opposite" | "ema_profit"
    TP_RR     = 2.0,           # for "rr": target = TP_RR x stop distance
    TP_VALUE  = 50.0,          # for "points" / "ticks"
    USE_TRAIL     = False,
    TRAIL_ACT_PTS = 20.0,      # trail activates after this many points in profit
    TRAIL_OFF_PTS = 10.0,      # trail distance behind the best price
    USE_BE          = False,
    BE_R            = 1.0,     # move stop to break-even after BE_R x 1R in favour (checked on bar close)
    BE_OFFSET_TICKS = 1,

    # ---- Time windows (NY) ----
    FIRST_ENTRY = (9, 30),     # no signals on bars starting before this
    LAST_ENTRY  = (15, 45),    # no signals on bars starting at/after this
    FLATTEN     = (16, 0),     # close everything at the last tick before this (multiple of timeframe)

    # ---- Filters (all OFF by default) ----
    MIN_DIST_PTS     = 0.0,    # min |EMA - VWAP| on the signal bar, 0 = off
    USE_VOL_FILTER   = False,
    VOL_LEN          = 20,
    VOL_MULT         = 1.2,    # signal bar volume >= SMA(volume) x mult
    USE_LOWVOL_WINDOW = False,
    LOWVOL_WINDOW    = ((11, 30), (13, 30)),   # no entries inside (NY lunch)
    USE_RELVOL       = False,
    RELVOL_LEN       = 50,
    RELVOL_MIN       = 0.6,    # SMA(vol,5) / SMA(vol,RELVOL_LEN) >= min

    # ---- Risk management ----
    CONTRACTS       = 1,
    RISK_PER_TRADE  = 0.0,     # $ risk per trade; > 0 sizes contracts from the stop distance
    MAX_CONTRACTS   = 10,
    MAX_TRADES_DAY  = 0,       # 0 = unlimited
    MAX_DAILY_LOSS  = 0.0,     # $ (realized + open at bar close), 0 = off -> flatten + stop for the day
)

# ============================================================
#  OPTIMIZER GRID — leave empty for a single run with PARAMS.
#  Every combination is simulated on the same loaded ticks (data is read once per day).
#  Keep it small: combos multiply quickly.
# ============================================================
GRID = {
    # "STOP_VALUE":   [15, 20, 25, 30, 40],
    # "TP_MODE":      ["none", "rr", "opposite"],
    # "MIN_DIST_PTS": [0, 2, 4, 6],
    # "LAST_ENTRY":   [(11, 30), (15, 45)],
    # "CONFIRM_CLOSE": [False, True],
}
GRID_RANK_BY    = "net_dd"  # "net" | "pf" | "net_dd" (net / max DD) | "expectancy"
GRID_MIN_TRADES = 100       # combos with fewer in-sample trades are not ranked
GRID_TOP_N      = 20

# ============================================================
NY_TZ = zoneinfo.ZoneInfo("America/New_York")


def build_combos():
    if not GRID:
        return [dict(PARAMS)]
    keys = list(GRID.keys())
    return [dict(PARAMS, **dict(zip(keys, vals))) for vals in itertools.product(*(GRID[k] for k in keys))]


COMBOS = build_combos()   # module level so multiprocessing workers (spawn on macOS) see the same list
# Any SESSION-anchored combo needs ticks from 18:00 NY the prior evening (prior file too).
NEEDS_SESSION = any(P["VWAP_ANCHOR"] == "SESSION" for P in COMBOS)


# ------------------------------------------------------------
#  Time helpers
# ------------------------------------------------------------
def ny_epoch(d, hour, minute):
    """NY wall-clock time on date d -> UTC epoch seconds."""
    return int(datetime(d.year, d.month, d.day, hour, minute, tzinfo=NY_TZ).timestamp())


def epoch_to_utc_int(ep):
    """Epoch -> integer YYYYMMDDHHMMSS (UTC), the tick files' timestamp format."""
    return int(datetime.fromtimestamp(ep, timezone.utc).strftime("%Y%m%d%H%M%S"))


def fmt_ny(ep):
    return datetime.fromtimestamp(ep, NY_TZ).strftime("%H:%M:%S")


_day_base = {}


def ts_to_epoch(ts_int):
    """Integer YYYYMMDDHHMMSS (UTC) -> epoch seconds, cached per calendar day for speed."""
    ymd, hms = divmod(ts_int, 1000000)
    base = _day_base.get(ymd)
    if base is None:
        y, md = divmod(ymd, 10000)
        mo, dd = divmod(md, 100)
        base = calendar.timegm((y, mo, dd, 0, 0, 0))
        _day_base[ymd] = base
    hh, ms = divmod(hms, 10000)
    mi, ss = divmod(ms, 100)
    return base + hh * 3600 + mi * 60 + ss


# ------------------------------------------------------------
#  Tick loading (+ binary cache)
# ------------------------------------------------------------
def _seek_to(f, size, target):
    """Binary-search a time-sorted tick file for the first line with timestamp >= target,
    so only the needed window is read (the overnight hours are skipped without reading them)."""
    lo, hi = 0, size
    while hi - lo > 1 << 16:
        mid = (lo + hi) // 2
        f.seek(mid)
        f.readline()                       # skip the partial line we landed in
        line = f.readline()
        while line and not line[:1].isdigit():
            line = f.readline()
        if not line or line[:14] >= target:
            hi = mid
        else:
            lo = mid
    f.seek(lo)
    if lo:
        f.readline()


def scan_file(filepath, start_ep, end_ep, ep, px, vol):
    """Append type=2 (trade) ticks within [start, end) to ep/px/vol.
    With FILES_SORTED, seeks to the window start and stops at the window end; otherwise
    scans the whole file with a fast pre-filter on the raw 14-char timestamp."""
    start_b = str(epoch_to_utc_int(start_ep)).encode()
    end_b = str(epoch_to_utc_int(end_ep)).encode()
    trade_b = TRADE_TYPE.encode()
    last = b""
    with open(filepath, "rb", buffering=1 << 20) as f:
        if FILES_SORTED:
            _seek_to(f, os.fstat(f.fileno()).st_size, start_b)
        for line in f:
            ts_raw = line[:14]
            if ts_raw < start_b:
                continue
            if ts_raw >= end_b:
                if FILES_SORTED and ts_raw[:1].isdigit():
                    break
                continue
            if FILES_SORTED:
                if ts_raw < last:
                    raise RuntimeError(f"{filepath} is not in time order — set FILES_SORTED = True ")
                last = ts_raw
            parts = line.split(b";")
            if len(parts) <= VOLUME_FIELD or parts[TYPE_FIELD] != trade_b:
                continue
            ep.append(ts_to_epoch(int(ts_raw)))
            px.append(float(parts[PRICE_FIELD]))
            vol.append(float(parts[VOLUME_FIELD]))


def load_day_ticks(date_str, files, start_ep, end_ep):
    """Trade ticks in [start_ep, end_ep) as (epoch array, price array, volume array).
    `files` may include the previous day's file (needed for the 18:00 session window — works
    whether your files are split by UTC day or by trading session). Cached as binary."""
    key = f"{date_str}_{start_ep}_{end_ep}"
    paths = [os.path.join(CACHE_DIR, f"{key}_{n}.bin") for n in ("ep", "px", "vol")]
    arrs = (array("q"), array("d"), array("d"))

    if all(os.path.exists(p) for p in paths):
        for p, a in zip(paths, arrs):
            with open(p, "rb") as f:
                a.frombytes(f.read())
        return arrs

    ep, px, vol = arrs
    for fp in files:
        if fp:
            scan_file(fp, start_ep, end_ep, ep, px, vol)
    if any(ep[i] > ep[i + 1] for i in range(len(ep) - 1)):
        order = sorted(range(len(ep)), key=ep.__getitem__)
        ep, px, vol = (array(a.typecode, (a[i] for i in order)) for a in arrs)
        arrs = (ep, px, vol)

    os.makedirs(CACHE_DIR, exist_ok=True)
    for p, a in zip(paths, arrs):
        tmp = f"{p}.{os.getpid()}.tmp"   # per-process tmp so parallel runs can't clobber
        with open(tmp, "wb") as f:
            f.write(a.tobytes())
        os.replace(tmp, p)
    return arrs


# ------------------------------------------------------------
#  Bars and indicators
# ------------------------------------------------------------
class Bar:
    __slots__ = ("t", "o", "h", "l", "c", "v", "pv", "i0", "i1")


class Day:
    """One trading day of ticks; builds bars per timeframe on demand."""

    def __init__(self, date_ny, ep, px, vol):
        self.date = date_ny
        self.ep, self.px, self.vol = ep, px, vol
        self.midnight = ny_epoch(date_ny, 0, 0)
        self.rth_open = ny_epoch(date_ny, 9, 30)
        self._bars = {}

    def at(self, hm):
        return ny_epoch(self.date, hm[0], hm[1])

    def bars(self, tf_sec):
        if tf_sec not in self._bars:
            self._bars[tf_sec] = build_bars(self.ep, self.px, self.vol, tf_sec, self.midnight)
        return self._bars[tf_sec]


def build_bars(ep, px, vol, tf_sec, align):
    """Aggregate ticks into time bars aligned to NY midnight. Empty periods produce no bar
    (same as TradingView). Each bar keeps its tick index range [i0, i1)."""
    bars = []
    cur = None
    key = None
    for k in range(len(ep)):
        t = ep[k]
        p = px[k]
        q = vol[k]
        bk = align + (t - align) // tf_sec * tf_sec
        if bk != key:
            if cur is not None:
                cur.i1 = k
                bars.append(cur)
            cur = Bar()
            cur.t = bk
            cur.o = cur.h = cur.l = cur.c = p
            cur.v = q
            cur.pv = p * q
            cur.i0 = k
            key = bk
        else:
            if p > cur.h:
                cur.h = p
            elif p < cur.l:
                cur.l = p
            cur.c = p
            cur.v += q
            cur.pv += p * q
    if cur is not None:
        cur.i1 = len(ep)
        bars.append(cur)
    return bars


def sma(values, length):
    out = [None] * len(values)
    s = 0.0
    for i, x in enumerate(values):
        s += x
        if i >= length:
            s -= values[i - length]
        if i >= length - 1:
            out[i] = s / length
    return out


def compute_indicators(bars, P, rth_open):
    """EMA (SMA-seeded like Pine's ta.ema), anchored VWAP, volume SMAs."""
    n = len(bars)
    L = P["EMA_LEN"]
    alpha = 2.0 / (L + 1)
    ema = [None] * n
    seed = 0.0
    for i, b in enumerate(bars):
        if i < L:
            seed += b.c
            if i == L - 1:
                ema[i] = seed / L
        else:
            ema[i] = alpha * b.c + (1 - alpha) * ema[i - 1]

    # VWAP: SESSION = anchored at the data window start (18:00 NY prior day);
    # RTH = reset on the first bar at/after 09:30 (bars before that only warm up the EMA).
    vwap = [None] * n
    reset = [False] * n
    cum_pv = cum_v = 0.0
    use_tick = P["VWAP_SOURCE"] == "tick"
    for i, b in enumerate(bars):
        if P["VWAP_ANCHOR"] == "RTH" and b.t >= rth_open and (i == 0 or bars[i - 1].t < rth_open):
            cum_pv = cum_v = 0.0
            reset[i] = True
        cum_pv += b.pv if use_tick else (b.h + b.l + b.c) / 3.0 * b.v
        cum_v += b.v
        vwap[i] = cum_pv / cum_v if cum_v > 0 else None

    vols = [b.v for b in bars]
    vol_sma = sma(vols, P["VOL_LEN"]) if P["USE_VOL_FILTER"] else None
    rel = None
    if P["USE_RELVOL"]:
        short, long_ = sma(vols, 5), sma(vols, P["RELVOL_LEN"])
        rel = [s / l if s is not None and l else None for s, l in zip(short, long_)]
    return ema, vwap, reset, vol_sma, rel


# ------------------------------------------------------------
#  Strategy simulation (one day, one parameter set)
# ------------------------------------------------------------
def stop_distance(P, price):
    m = P["STOP_MODE"]
    if m == "ticks":
        return P["STOP_VALUE"] * TICK_SIZE
    if m == "pct":
        return price * P["STOP_VALUE"] / 100.0
    return P["STOP_VALUE"]


def target_distance(P, risk):
    m = P["TP_MODE"]
    if m == "rr":
        return risk * P["TP_RR"]
    if m == "points":
        return P["TP_VALUE"]
    if m == "ticks":
        return P["TP_VALUE"] * TICK_SIZE
    return None


def simulate_day(day, P):
    """Bar-by-bar port of the Pine logic. Order of events per bar:
         1) market orders queued at the previous bar's close fill at this bar's first tick
         2) stop / target / trailing stop checked on every tick of the bar
         3) at the bar close: daily-loss check, break-even, close-based exits, new signals"""
    tf_sec = P["TIMEFRAME_MIN"] * 60
    flatten = day.at(P["FLATTEN"])
    bars = [b for b in day.bars(tf_sec) if b.t < flatten]
    n = len(bars)
    if n < P["EMA_LEN"] + 2:
        return []
    ema, vwap, reset, vol_sma, rel = compute_indicators(bars, P, day.rth_open)
    ep, px = day.ep, day.px

    first_entry = day.at(P["FIRST_ENTRY"])
    last_entry = day.at(P["LAST_ENTRY"])
    lv0, lv1 = day.at(P["LOWVOL_WINDOW"][0]), day.at(P["LOWVOL_WINDOW"][1])
    allow_long = P["DIRECTION"] != "short"
    allow_short = P["DIRECTION"] != "long"
    slip = SLIPPAGE_TICKS * TICK_SIZE
    be_off = P["BE_OFFSET_TICKS"] * TICK_SIZE
    tp_mode = P["TP_MODE"]

    results = []
    pos = None            # open position dict
    pend_entry = 0        # +1 / -1 queued for next bar open
    pend_exit = None      # exit reason queued for next bar open
    trades_today = 0
    realized = 0.0
    locked = False

    def close(price, t, why):
        nonlocal pos, realized
        side, qty = pos["side"], pos["qty"]
        pts = (price - pos["entry"]) * side
        gross = pts * POINT_VALUE * qty
        comm = COMMISSION_PER_CONTRACT * qty * 2
        net = gross - comm
        realized += net
        results.append({
            "date": day.date.strftime("%Y-%m-%d"),
            "side": "long" if side == 1 else "short",
            "qty": qty,
            "entry_time": fmt_ny(pos["t"]),
            "entry_price": round(pos["entry"], 2),
            "exit_time": fmt_ny(t),
            "exit_price": round(price, 2),
            "exit_type": why,
            "pnl_points": round(pts, 2),
            "risk_points": round(pos["risk"], 2),
            "r_multiple": round(pts / pos["risk"], 2) if pos["risk"] > 0 else None,
            "mfe_points": round(pos["mfe"], 2),
            "mae_points": round(pos["mae"], 2),
            "gross": round(gross, 2),
            "commission": round(comm, 2),
            "net": round(net, 2),
            "entry_epoch": pos["t"],
        })
        pos = None

    for i in range(n):
        b = bars[i]
        start_k = b.i0

        # ---- 1) fills at the open (first tick) of this bar ----
        if pend_exit is not None and pos is not None:
            close(px[b.i0] - pos["side"] * slip, ep[b.i0], pend_exit)
        pend_exit = None

        if pend_entry and pos is None:
            side = pend_entry
            entry = px[b.i0] + side * slip
            risk = stop_distance(P, entry)
            qty = P["CONTRACTS"]
            if P["RISK_PER_TRADE"] > 0:
                qty = int(P["RISK_PER_TRADE"] // (risk * POINT_VALUE))
                qty = max(1, min(qty, P["MAX_CONTRACTS"]))
            tgt_d = target_distance(P, risk)
            pos = {
                "side": side, "qty": qty, "entry": entry, "t": ep[b.i0], "risk": risk,
                "stop": entry - side * risk if P["USE_FIXED_STOP"] else None,
                "target": entry + side * tgt_d if tgt_d is not None else None,
                "be": False, "mfe": 0.0, "mae": 0.0,
            }
            trades_today += 1
        pend_entry = 0

        # ---- 2) intrabar: hard stop / BE stop, trailing stop, take-profit on every tick ----
        if pos is not None:
            side = pos["side"]
            entry = pos["entry"]
            tgt = pos["target"]
            for k in range(start_k, b.i1):
                p = px[k]
                fav = (p - entry) * side
                if fav > pos["mfe"]:
                    pos["mfe"] = fav
                elif -fav > pos["mae"]:
                    pos["mae"] = -fav

                stop = pos["stop"]
                why = "BE" if pos["be"] else "SL"
                if P["USE_TRAIL"] and pos["mfe"] >= P["TRAIL_ACT_PTS"]:
                    trail = entry + side * (pos["mfe"] - P["TRAIL_OFF_PTS"])
                    if stop is None or (trail - stop) * side > 0:
                        stop, why = trail, "Trail"

                if stop is not None and (p - stop) * side <= 0:
                    close(p - side * slip, ep[k], why)      # stop -> market, fills at the tick
                    break
                if tgt is not None and ((p - tgt) * side > 0 if TP_TRADE_THROUGH else (p - tgt) * side >= 0):
                    close(tgt, ep[k], "TP")                  # limit fills at its price
                    break

        # ---- 3) bar close logic ----
        c = b.c
        e, v = ema[i], vwap[i]
        if e is None or v is None or i == 0 or ema[i - 1] is None or vwap[i - 1] is None:
            continue
        bull = e > v and ema[i - 1] <= vwap[i - 1]
        bear = e < v and ema[i - 1] >= vwap[i - 1]
        if reset[i] and P["IGNORE_RESET_CROSS"]:
            bull = bear = False

        if P["MAX_DAILY_LOSS"] > 0 and not locked:
            open_pnl = 0.0
            if pos is not None:
                open_pnl = (c - pos["entry"]) * pos["side"] * POINT_VALUE * pos["qty"]
            if realized + open_pnl <= -P["MAX_DAILY_LOSS"]:
                locked = True
                if pos is not None:
                    pend_exit = "Daily Loss"

        if pos is not None and pend_exit is None:
            side = pos["side"]
            # break-even, active from the next bar (same as Pine)
            if P["USE_BE"] and not pos["be"] and pos["mfe"] >= P["BE_R"] * pos["risk"]:
                be_level = pos["entry"] + side * be_off
                if pos["stop"] is None or (be_level - pos["stop"]) * side > 0:
                    pos["stop"] = be_level
                pos["be"] = True

            through_ema = (c - e) * side < 0
            in_profit = (c - pos["entry"]) * side > 0
            if P["USE_EMA_STOP"] and through_ema:
                pend_exit = "EMA Exit" if in_profit else "EMA SL"
            elif tp_mode == "opposite" and (bear if side == 1 else bull):
                pend_exit = "Opp Cross"
            elif tp_mode == "ema_profit" and through_ema and in_profit:
                pend_exit = "EMA TP"

        # entries: flat, inside the entry window, filters pass, and a next bar exists to fill on
        if pos is None and not locked and i + 1 < n and first_entry <= b.t < last_entry:
            if P["MAX_TRADES_DAY"] and trades_today >= P["MAX_TRADES_DAY"]:
                continue
            if P["MIN_DIST_PTS"] > 0 and abs(e - v) < P["MIN_DIST_PTS"]:
                continue
            if P["USE_VOL_FILTER"] and (vol_sma[i] is None or b.v < vol_sma[i] * P["VOL_MULT"]):
                continue
            if P["USE_LOWVOL_WINDOW"] and lv0 <= b.t < lv1:
                continue
            if P["USE_RELVOL"] and (rel[i] is None or rel[i] < P["RELVOL_MIN"]):
                continue
            if bull and allow_long and (not P["CONFIRM_CLOSE"] or (c > e and c > v)):
                pend_entry = 1
            elif bear and allow_short and (not P["CONFIRM_CLOSE"] or (c < e and c < v)):
                pend_entry = -1

    # ---- end of day: flatten at the last tick before FLATTEN ----
    if pos is not None:
        k = bars[-1].i1 - 1
        close(px[k] - pos["side"] * slip, ep[k], "EOD")
    return results


# ------------------------------------------------------------
#  Files and workers
# ------------------------------------------------------------
def find_all_csv_files(data_dir):
    """Find all daily CSV files, sorted by date (same layout rules as ORB_Backtest.py)."""
    files = []
    direct_csvs = [f for f in os.listdir(data_dir) if f.endswith(".csv") and f[0].isdigit()]
    if direct_csvs:
        for fname in sorted(direct_csvs):
            files.append(os.path.join(data_dir, fname))
    else:
        for folder in sorted(os.listdir(data_dir)):
            folder_path = os.path.join(data_dir, folder)
            if not os.path.isdir(folder_path):
                continue
            for fname in sorted(os.listdir(folder_path)):
                if fname.endswith(".csv") and fname[0].isdigit():
                    files.append(os.path.join(folder_path, fname))
    files.sort(key=os.path.basename)
    return files


def file_date(filepath):
    s = os.path.basename(filepath).replace(".csv", "")
    try:
        return s, datetime(int(s[0:4]), int(s[4:6]), int(s[6:8])).date()
    except ValueError:
        return s, None


def process_day(task):
    """Worker: load one day's ticks once, run every parameter combo on them."""
    filepath, prev_filepath = task
    date_str, d = file_date(filepath)
    if d is None or d.weekday() >= 5:
        return date_str, None

    end_ep = max(ny_epoch(d, *P["FLATTEN"]) for P in COMBOS)
    if NEEDS_SESSION:
        prev = d - timedelta(days=1)
        start_ep = ny_epoch(prev, 18, 0)
        files = [prev_filepath, filepath]
    else:
        start_ep = ny_epoch(d, *WARMUP_START)
        files = [filepath]

    ep, px, vol = load_day_ticks(date_str, files, start_ep, end_ep)
    if not ep:
        return date_str, None
    day = Day(d, ep, px, vol)
    return date_str, [simulate_day(day, P) for P in COMBOS]


# ------------------------------------------------------------
#  Statistics and reporting
# ------------------------------------------------------------
def compute_stats(trades):
    n = len(trades)
    if n == 0:
        return None
    nets = [t["net"] for t in trades]
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x < 0]
    gp, gl = sum(wins), -sum(losses)
    total = sum(nets)

    cum = pk = dd = 0.0               # trade-by-trade equity drawdown
    for x in nets:
        cum += x
        pk = max(pk, cum)
        dd = max(dd, pk - cum)

    daily = defaultdict(float)
    for t in trades:
        daily[t["date"]] += t["net"]

    cw = cl = mw = ml = 0
    for x in nets:
        if x > 0:
            cw += 1; cl = 0
        elif x < 0:
            cl += 1; cw = 0
        else:
            cw = cl = 0
        mw, ml = max(mw, cw), max(ml, cl)

    avg_win = gp / len(wins) if wins else 0.0
    avg_loss = -gl / len(losses) if losses else 0.0
    rs = [t["r_multiple"] for t in trades if t["r_multiple"] is not None]
    return {
        "trades": n, "net": total, "gross": sum(t["gross"] for t in trades),
        "commission": sum(t["commission"] for t in trades),
        "wins": len(wins), "losses": len(losses),
        "win_rate": len(wins) / n * 100,
        "avg_win": avg_win, "avg_loss": avg_loss,
        "rr": avg_win / -avg_loss if avg_loss < 0 else float("inf"),
        "pf": gp / gl if gl > 0 else float("inf"),
        "expectancy": total / n,
        "avg_r": sum(rs) / len(rs) if rs else 0.0,
        "max_dd": dd,
        "net_dd": total / dd if dd > 0 else float("inf"),
        "days": len(daily),
        "green": sum(1 for v in daily.values() if v > 0),
        "red": sum(1 for v in daily.values() if v < 0),
        "avg_daily": total / len(daily) if daily else 0.0,
        "max_cw": mw, "max_cl": ml,
    }


def split_is_oos(trades):
    if not OOS_START:
        return trades, []
    return ([t for t in trades if t["date"] < OOS_START],
            [t for t in trades if t["date"] >= OOS_START])


def print_report(trades, title):
    cols = [("ALL", trades),
            ("LONG", [t for t in trades if t["side"] == "long"]),
            ("SHORT", [t for t in trades if t["side"] == "short"])]
    stats = [(name, compute_stats(ts)) for name, ts in cols]

    def money(x):
        return "${:,.2f}".format(x)

    def num(x, f="{:.2f}"):
        return "inf" if x == float("inf") else f.format(x)

    rows = [
        ("Total Trades", lambda s: str(s["trades"])),
        ("Net P&L (after comm.)", lambda s: money(s["net"])),
        ("Gross P&L (incl. slippage)", lambda s: money(s["gross"])),
        ("Win Rate", lambda s: "{:.1f}% ({}W/{}L)".format(s["win_rate"], s["wins"], s["losses"])),
        ("Profit Factor", lambda s: num(s["pf"])),
        ("Expectancy / Trade", lambda s: money(s["expectancy"])),
        ("Avg Win", lambda s: money(s["avg_win"])),
        ("Avg Loss", lambda s: money(s["avg_loss"])),
        ("Avg Win / Avg Loss", lambda s: num(s["rr"])),
        ("Avg R Multiple", lambda s: num(s["avg_r"])),
        ("Max Drawdown", lambda s: money(s["max_dd"])),
        ("Net / Max DD", lambda s: num(s["net_dd"])),
        ("Green / Red Days", lambda s: "{}/{} of {}".format(s["green"], s["red"], s["days"])),
        ("Avg Daily P&L", lambda s: money(s["avg_daily"])),
        ("Max Consec W / L", lambda s: "{} / {}".format(s["max_cw"], s["max_cl"])),
        ("Total Commission", lambda s: money(s["commission"])),
    ]
    print("\n" + "=" * 86)
    print(f"  {title}")
    print("=" * 86)
    print(f"  {'':28s}" + "".join(f"{name:>19s}" for name, _ in stats))
    print(f"  {'-' * 28}" + " " + "-" * 56)
    for label, fn in rows:
        print(f"  {label:28s}" + "".join(f"{(fn(s) if s else 'N/A'):>19s}" for _, s in stats))

    by_exit = defaultdict(list)
    for t in trades:
        by_exit[t["exit_type"]].append(t["net"])
    print(f"\n  {'Exit type':16s}{'Count':>8s}{'Win %':>9s}{'Net':>15s}{'Avg':>12s}")
    for k in sorted(by_exit, key=lambda k: -len(by_exit[k])):
        v = by_exit[k]
        print(f"  {k:16s}{len(v):>8d}{sum(1 for x in v if x > 0) / len(v) * 100:>8.1f}%"
              f"{money(sum(v)):>15s}{money(sum(v) / len(v)):>12s}")


def print_settings():
    P = PARAMS
    print("=" * 86)
    print("  VWAP + EMA CROSSOVER BACKTESTER — Tick Data")
    print("=" * 86)
    print(f"  Bars: {P['TIMEFRAME_MIN']}m | EMA {P['EMA_LEN']} | VWAP {P['VWAP_ANCHOR']} ({P['VWAP_SOURCE']}) | "
          f"Direction: {P['DIRECTION']} | Confirm close: {P['CONFIRM_CLOSE']}")
    print(f"  Stops: EMA exit {'ON' if P['USE_EMA_STOP'] else 'OFF'} | Fixed {'ON' if P['USE_FIXED_STOP'] else 'OFF'} "
          f"({P['STOP_VALUE']} {P['STOP_MODE']}) | TP: {P['TP_MODE']} | Trail {'ON' if P['USE_TRAIL'] else 'OFF'} | "
          f"BE {'ON' if P['USE_BE'] else 'OFF'}")
    print(f"  Entries {P['FIRST_ENTRY'][0]}:{P['FIRST_ENTRY'][1]:02d}-{P['LAST_ENTRY'][0]}:{P['LAST_ENTRY'][1]:02d} NY, "
          f"flatten {P['FLATTEN'][0]}:{P['FLATTEN'][1]:02d} | Size {P['CONTRACTS']} | ${POINT_VALUE}/pt | "
          f"Comm ${COMMISSION_PER_CONTRACT}/side | Slippage {SLIPPAGE_TICKS} tick(s)")
    if GRID:
        print(f"  GRID: {len(COMBOS)} combos over {', '.join(GRID.keys())} (above = base values)")
    if OOS_START:
        print(f"  Out-of-sample from {OOS_START}")
    print("=" * 86)


def write_grid(per_combo):
    keys = list(GRID.keys())
    rows = []
    for P, trades in zip(COMBOS, per_combo):
        ins, oos = split_is_oos(trades)
        si, so = compute_stats(ins), compute_stats(oos)
        row = {k: P[k] for k in keys}
        for prefix, s in (("is_", si), ("oos_", so)):
            for m in ("trades", "net", "win_rate", "pf", "expectancy", "max_dd", "net_dd", "avg_r"):
                row[prefix + m] = round(s[m], 3) if s and s[m] != float("inf") else (s[m] if s else None)
        rows.append(row)

    def rank_value(r):
        v = r.get("is_" + GRID_RANK_BY)
        return v if isinstance(v, (int, float)) else float("-inf")

    ranked = sorted([r for r in rows if (r["is_trades"] or 0) >= GRID_MIN_TRADES], key=rank_value, reverse=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(GRID_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(sorted(rows, key=rank_value, reverse=True))

    print("\n" + "=" * 86)
    print(f"  GRID — top {GRID_TOP_N} by in-sample {GRID_RANK_BY} (min {GRID_MIN_TRADES} trades)")
    print("=" * 86)
    hdr = "  " + " ".join(f"{k[:14]:>14s}" for k in keys) + f"{'IS trd':>8s}{'IS net':>11s}{'IS PF':>7s}{'IS DD':>10s}"
    if OOS_START:
        hdr += f"{'OOS trd':>8s}{'OOS net':>11s}{'OOS PF':>7s}"
    print(hdr)
    for r in ranked[:GRID_TOP_N]:
        line = "  " + " ".join(f"{str(r[k])[:14]:>14s}" for k in keys)
        line += f"{r['is_trades']:>8d}{r['is_net']:>11,.0f}{str(r['is_pf'])[:6]:>7s}{r['is_max_dd']:>10,.0f}"
        if OOS_START:
            line += (f"{r['oos_trades']:>8d}{r['oos_net']:>11,.0f}{str(r['oos_pf'])[:6]:>7s}"
                     if r["oos_trades"] else f"{'-':>8s}{'-':>11s}{'-':>7s}")
        print(line)
    if not ranked:
        print("  No combo reached GRID_MIN_TRADES.")
    print(f"\n  Full grid saved to: {GRID_CSV}")


def main():
    t0 = time.time()
    print_settings()

    csv_files = find_all_csv_files(TICK_DATA_DIR)
    tasks = []
    for i, fp in enumerate(csv_files):
        ds, d = file_date(fp)
        if d is None:
            continue
        iso = d.isoformat()
        if (START_DATE and iso < START_DATE) or (END_DATE and iso > END_DATE):
            continue
        tasks.append((fp, csv_files[i - 1] if i > 0 else None))

    workers = WORKERS
    print(f"\n  {len(tasks)} daily files — {len(COMBOS)} combo(s) — {workers} workers\n")

    per_combo = [[] for _ in COMBOS]
    with Pool(workers) as pool:
        for done, (date_str, day_results) in enumerate(pool.imap(process_day, tasks), 1):
            elapsed = time.time() - t0
            eta = elapsed / done * (len(tasks) - done)
            progress = f"[{done}/{len(tasks)} {elapsed / 60:.0f}m, ~{eta / 60:.0f}m left]"
            if not day_results:
                continue
            for ci, res in enumerate(day_results):
                per_combo[ci].extend(res)
            if not GRID and day_results[0]:
                res = day_results[0]
                print(f"  {progress} {date_str}  ${sum(r['net'] for r in res):+9.2f}  " + ", ".join(
                    f"{r['side'][0].upper()} {r['entry_time'][:5]}→{r['exit_type']} {r['net']:+.0f}" for r in res))
            elif GRID and (done % 10 == 0 or done == len(tasks)):
                print(f"  {progress} {date_str}")

    for trades in per_combo:
        trades.sort(key=lambda r: r["entry_epoch"])

    if GRID:
        write_grid(per_combo)
    else:
        trades = per_combo[0]
        if not trades:
            print("  No trades found!")
            return
        ins, oos = split_is_oos(trades)
        print_report(ins, "RESULTS — IN-SAMPLE" if OOS_START else "RESULTS SUMMARY")
        if oos:
            print_report(oos, f"RESULTS — OUT-OF-SAMPLE (from {OOS_START})")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(TRADES_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[k for k in trades[0] if k != "entry_epoch"],
                                    extrasaction="ignore")
            writer.writeheader()
            writer.writerows(trades)
        print(f"\n  Trades saved to: {TRADES_CSV}")

    print(f"  Completed in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
