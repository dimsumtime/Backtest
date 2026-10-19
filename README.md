# VWAP + 9 EMA Crossover Strategy (Pine Script v6)

`vwap_ema9_crossover_strategy.pine` is a TradingView `strategy()` for NQ, MNQ, ES (and other futures).
Paste it into the Pine Editor, then click **Add to chart**.

## Entry logic
- **Long:** EMA(9) crosses above session VWAP on a closed candle, and you have no open position.
- **Short:** EMA(9) crosses below session VWAP on a closed candle, and you have no open position.
- **Optional candle-close confirmation:** the signal candle must also close beyond both lines.
- Market orders fill at the next bar's open. There is no pyramiding and no reversal.

## Exit logic
| Exit | Default |
|---|---|
| Stop method 1: candle closes back through EMA(9) | ON |
| Stop method 2: fixed stop (% of position value, points or ticks) | ON, 25 % |
| Take-profit: R:R 1:1, 1:1.5, 1:2 or 1:3, points, ticks, opposite crossover, or EMA close while in profit | None |
| Trailing stop | OFF |
| Break-even after N R | OFF |
| Max daily loss / max trades per day | OFF |

Every filter is OFF by default: session, direction, minimum EMA–VWAP distance, volume, low-volume window, relative volume, and higher signal timeframe.

## Recommended backtest settings (3-minute NQ)
- Chart: `NQ1!` on 3 min. Use ETH data with the VWAP anchor set to `RTH (09:30 NY)`, or use RTH data with `Session`.
- Fixed stop: switch it to **Points**, about 20–30 points. At NQ ≈ 20,000, 25 % of position value is about 5,000 points, so that stop works only as a disaster stop.
- Properties tab:
  - Initial capital $50k, 1 contract.
  - Commission about $2.10 per contract per side for NQ/ES, or about $0.62 for MNQ.
  - Slippage 1–2 ticks.
  - Turn on Bar Magnifier if your plan has it.
- Test the baseline first. Then add one filter at a time, such as a 09:30–15:45 session with flatten, and compare out-of-sample periods.
