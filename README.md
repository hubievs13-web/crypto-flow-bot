# crypto-flow-bot

Telegram alerts on **real Binance flow data** (funding rate, open interest, top-trader long/short ratio, liquidation cascades) — with a built-in **virtual position tracker** that follows up every entry signal with **SL / TP ladder / trailing stop / time stop / reason-invalidation** exit alerts.

The bot does **not** execute trades. It watches the market, sends you signals, and tells you when to close. You stay in control of the actual orders on Binance (or wherever).

---

## What it watches

For each symbol in `config.yaml`:

| Metric | Source | Why it matters |
|---|---|---|
| **Funding rate** | Binance `/fapi/v1/premiumIndex` | High positive funding = crowded longs. |
| **Open Interest** | Binance `/fapi/v1/openInterest` + history | Surge in OI = fresh capital entering futures. |
| **Top-trader L/S ratio** | Binance `/futures/data/topLongShortPositionRatio` | Crowd positioning. |
| **Liquidations** | Binance WS `!forceOrder@arr` | Forced closes — often precede squeezes / bounces. |

When one or more of these crosses a configurable threshold, the bot opens a **virtual position** internally and sends you a Telegram message with the entry, SL, TP ladder, trailing rule, and time stop.

It then keeps watching and sends a follow-up alert when:
- price hits **SL** (close all)
- price hits **TP1 / TP2** (fix that fraction)
- **trailing** triggers (move SL to break-even or further)
- **time stop** fires
- the **original reason is invalidated** (e.g. funding back to neutral) — close at break-even

All entry/exit alerts are also written to `logs/alerts.jsonl` so you can backtest later: which alerts actually preceded a useful move, which were noise, and tune thresholds.

---

## Quick start

### 1. Get a Telegram bot

1. Open Telegram, talk to [@BotFather](https://t.me/BotFather), send `/newbot`.
2. Save the token it returns — that's `TELEGRAM_BOT_TOKEN`.
3. Open [@userinfobot](https://t.me/userinfobot), send `/start`. The number it shows is your `TELEGRAM_CHAT_IDS`.
   - For groups/channels, add the bot to the group, send any message, then visit `https://api.telegram.org/bot<TOKEN>/getUpdates` and read the `chat.id` from the JSON.

### 2. Local run (Python ≥ 3.11)

```bash
git clone https://github.com/hubievs13-web/crypto-flow-bot.git
cd crypto-flow-bot

python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env
# edit .env — set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS

python -m crypto_flow_bot
```

### 3. Run on Fly.io (recommended for 24/7)

```bash
# Install flyctl: https://fly.io/docs/hands-on/install-flyctl/
fly auth login

# Inside the repo:
fly launch --no-deploy --copy-config
# When prompted to use the existing fly.toml, accept; pick a region near you.

# One-time persistent volume for state.json + logs:
fly volumes create state --size 1 --region fra

# Secrets:
fly secrets set TELEGRAM_BOT_TOKEN=123:abc TELEGRAM_CHAT_IDS=12345678

fly deploy
fly logs           # watch the bot run
```

After a successful deploy the app keeps running 24/7 on Fly's free allowance.

### 4. Run on a Hetzner / DigitalOcean VPS (alternative)

```bash
# On the VPS as root:
adduser --disabled-password --gecos "" botuser
apt-get update && apt-get install -y python3.11 python3.11-venv git
sudo -iu botuser

git clone https://github.com/hubievs13-web/crypto-flow-bot.git /opt/crypto-flow-bot
cd /opt/crypto-flow-bot
python3.11 -m venv .venv
.venv/bin/pip install -e .
cp .env.example .env  # then edit with your secrets

mkdir -p state logs
exit

# As root:
cp /opt/crypto-flow-bot/deploy/crypto-flow-bot.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now crypto-flow-bot
journalctl -u crypto-flow-bot -f
```

---

## Configuration

All thresholds live in `config.yaml`. See the inline comments — every field has a one-line explanation in plain English. The defaults are conservative starting points; expect to tune them after a few days of observing alerts.

Highlights:

- **`signals.funding_extreme`** — fire when 8h funding crosses ±X%.
- **`signals.oi_surge`** — fire when OI changes by more than X% over a window.
- **`signals.lsr_extreme`** — fire when top-trader long/short ratio is crowded.
- **`signals.liq_cascade`** — fire when one-sided liquidations exceed $X within a window.
- **`exits.stop_loss_pct`** — hard SL distance.
- **`exits.take_profit_levels`** — list of `(pct, fraction)` partial-take levels.
- **`exits.trailing`** — when to engage the trailing stop, and what to lock in.
- **`exits.time_stop_minutes`** — auto-close after this long.
- **`exits.reason_invalidation`** — close when the original metric normalizes.

To override only some fields, copy `config.yaml` to `config.local.yaml`, edit, and point the bot at it via `CRYPTO_FLOW_BOT_CONFIG=/path/to/config.local.yaml`.

---

## Operational notes

- **The bot does not place orders.** Every alert is informational. You decide whether to act.
- **Cooldown.** After an alert for a (symbol, direction) pair, the bot waits `alert_cooldown_seconds` before sending another for the same pair.
- **One side at a time.** If a LONG virtual position is open for ETH, a new LONG signal is suppressed until it closes. SHORT signals on the same symbol are also suppressed while the LONG is open (avoid whipsaw spam).
- **State persists.** `state/state.json` keeps open virtual positions and last-alert timestamps across restarts. On Fly.io it lives on a volume; on a VPS in `/opt/crypto-flow-bot/state/`.
- **Logs are JSONL.** `logs/snapshots.jsonl`, `logs/alerts.jsonl`, `logs/positions.jsonl` are append-only. Convert to Parquet later for backtests:

  ```python
  import pandas as pd
  pd.read_json("logs/alerts.jsonl", lines=True).to_parquet("alerts.parquet")
  ```

---

## Limits & honest disclaimers

- These signals are **statistical heuristics**, not a holy grail. Funding/OI/LSR extremes give a *bias*, not certainty. Backtest before risking real capital.
- The Binance liquidation websocket only reports liquidations on **Binance USD-M Futures**. Liquidations on other venues (OKX, Bybit, etc.) aren't captured. For a fuller picture you'd add Coinglass aggregation.
- Connectivity matters. Run the bot on a stable VPS / Fly.io — laptop sleep means missed signals.
- Telegram has rate limits (~30 messages/sec to a group). The default thresholds are tuned to keep alert frequency low.

---

## Development

```bash
pip install -e ".[dev]"
ruff check .
pytest
```

License: MIT.
