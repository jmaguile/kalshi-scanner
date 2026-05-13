# kalshi-scanner

A Flask-based live scanner for Kalshi prediction markets. Tracks real-time BTC 15-minute settlement markets with orderbook depth, imbalance metrics, CSV logging, and optional market order execution.

## Features

- Live YES/NO price feed for KXBTC15M markets
- Orderbook depth visualization with bid/ask imbalance
- BTC spot price overlay via Coinbase
- SSE-based real-time browser updates (no page refresh)
- CSV tick logging per market window
- Optional market order placement (disabled by default)

## Setup

### 1. Clone and install dependencies

```bash
pip install flask requests cryptography
```

### 2. Configure credentials

Copy `.env.example` to `.env` and fill in your Kalshi API credentials:

```bash
cp .env.example .env
```

Set the following environment variables (or edit the `CONFIG` section at the top of `kalshi_scanner.py`):

| Variable | Description |
|---|---|
| `KALSHI_KEY_ID` | Your Kalshi API key ID |
| `KALSHI_PEM_PATH` | Path to your RSA private key `.pem` file |

You can generate an RSA key pair and register the public key in your [Kalshi account settings](https://kalshi.com/account/api).

### 3. Run

```bash
python kalshi_scanner.py
```

Open `http://localhost:5000` in your browser.

## Enable Live Trading

Trading is disabled by default. To enable real order placement, open `kalshi_scanner.py` and uncomment:

```python
TRADING_ENABLED = True
```

> ⚠️ When enabled, button clicks place real market orders on Kalshi. Real money is at risk. Test with quantity = 1 first.

## Project Structure

```
kalshi-scanner/
├── kalshi_scanner.py   # Main app
├── logs/               # CSV tick and order logs (auto-created)
├── .env.example        # Credential template
└── README.md
```

## Notes

- RSA-PSS signing is required for populated bid/ask/volume fields. Without it, Kalshi returns null values.
- The scanner polls at 4x/second by default. Adjust via `poller.start(interval=...)`.
- Tick CSVs rotate per market window. Orders log to a single rolling `orders.csv`.
