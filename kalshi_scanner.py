"""
kalshi_scanner.py
─────────────────
Flask-based Kalshi live scanner. Runs on your server.
Open http://localhost:5000 in your browser.

Run:
    conda activate your_env
    pip install flask requests cryptography
    python kalshi_scanner.py
"""

import atexit
import base64
import csv
import json
import os
import queue
import sys
import threading
import time
import uuid
from pathlib import Path

import requests
from datetime import datetime, timezone
from collections import deque
from flask import Flask, Response, jsonify, render_template_string, request as freq


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
MAX_BARS = 5000
HOST     = "0.0.0.0"
PORT     = 5000

# Kalshi RSA auth: key ID + path to your private .pem file.
# Without RSA signing, Kalshi returns null bid/ask → chart flat-lines at 50¢.
KALSHI_KEY_ID  = os.environ.get("KALSHI_KEY_ID", "")
KALSHI_PEM_PATH = os.environ.get("KALSHI_PEM_PATH", "kalshi_private.pem")

# Kalshi BTC series. KXBTC15M = 15-minute markets, KXBTC = hourly price-range.
SERIES = "KXBTC15M"

# Directory where CSV logs are written.
# Ticks per ticker → ticks_{ticker}_{starttime}.csv  (new file each market)
# Orders         → orders.csv                        (rolling, all sessions)
CSV_DIR = "logs"


# ═══════════════════════════════════════════════════════════════════════════════
# ██████████████████████████████████████████████████████████████████████████████
# █                                                                             █
# █   💰  ENABLE LIVE TRADING                                                    █
# █   ────────────────────────                                                  █
# █                                                                             █
# █   To arm the BUY YES / BUY NO buttons, UNCOMMENT the single line below.     █
# █                                                                             █
# █   ⚠️  WHEN UNCOMMENTED, BUTTON CLICKS PLACE REAL MARKET ORDERS ON KALSHI.    █
# █   ⚠️  REAL MONEY IS AT RISK. Test with quantity = 1 first.                   █
# █                                                                             █
# ██████████████████████████████████████████████████████████████████████████████
# ═══════════════════════════════════════════════════════════════════════════════

TRADING_ENABLED = False
# TRADING_ENABLED = True   # ←──── UNCOMMENT THIS LINE TO ENABLE BUY BUTTONS


# ─────────────────────────────────────────────────────────────
# KALSHI API
# ─────────────────────────────────────────────────────────────
class KalshiAPI:
    """RSA-PSS signed Kalshi REST client. The 'API key' string is actually the
    key ID; signing each request with the matching .pem private key is what
    proves identity and unlocks populated bid/ask/volume fields."""

    def __init__(self, key_id: str, pem_path: str):
        from cryptography.hazmat.primitives import serialization
        with open(pem_path, "rb") as f:
            self._private_key = serialization.load_pem_private_key(f.read(), password=None)
        self._key_id = key_id
        self.session = requests.Session()

    def _sign(self, msg: str) -> str:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        sig = self._private_key.sign(
            msg.encode("utf-8"),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def _headers(self, method: str, path: str) -> dict:
        ts = str(int(time.time() * 1000))
        msg = ts + method.upper() + path
        return {
            "KALSHI-ACCESS-KEY":       self._key_id,
            "KALSHI-ACCESS-SIGNATURE": self._sign(msg),
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "Content-Type":            "application/json",
        }

    def _get(self, path, params=None):
        # The signed path includes the API prefix but NOT the query string.
        signed_path = "/trade-api/v2" + path
        r = self.session.get(BASE_URL + path, params=params,
                             headers=self._headers("GET", signed_path), timeout=10)
        r.raise_for_status()
        return r.json()

    def get_active_15m(self, series: str):
        now  = datetime.now(timezone.utc)
        data = self._get("/markets", {"series_ticker": series,
                                      "status": "open", "limit": 1000})
        mkts = data.get("markets", [])
        up   = [m for m in mkts if datetime.fromisoformat(
                    m["close_time"].replace("Z", "+00:00")) > now]
        if not up:
            return mkts[0] if mkts else None

        soonest = min(m["close_time"] for m in up)
        same_close = [m for m in up if m["close_time"] == soonest]
        def yes_bid_value(m):
            v = m.get("yes_bid_dollars") or m.get("yes_bid") or 0
            try: return float(v)
            except (TypeError, ValueError): return 0
        return max(same_close, key=yes_bid_value)

    def get_market(self, ticker: str):
        return self._get(f"/markets/{ticker}")["market"]

    def get_orderbook(self, ticker: str):
        data = self._get(f"/markets/{ticker}/orderbook")
        # Kalshi's current schema wraps the book in `orderbook_fp` with
        # `yes_dollars` / `no_dollars` keys (price-dollar strings, qty-float
        # strings). Old responses used `orderbook` / `yes` / `no`.
        ob = data.get("orderbook_fp") or data.get("orderbook")
        return ob if isinstance(ob, dict) else {}

    def _post(self, path, payload):
        signed_path = "/trade-api/v2" + path
        r = self.session.post(BASE_URL + path, json=payload,
                              headers=self._headers("POST", signed_path), timeout=10)
        if not r.ok:
            # Surface Kalshi's actual error body — the bare status code is useless
            try:    body = r.json()
            except Exception: body = r.text
            raise requests.HTTPError(f"{r.status_code} {r.reason} from {path}: {body}",
                                     response=r)
        return r.json()

    def place_order(self, ticker: str, side: str, count: int,
                    action: str = "buy", order_type: str = "market"):
        """Place a market order on Kalshi.

        side    = 'yes' or 'no'   (which contract you're buying or selling)
        count   = number of contracts
        action  = 'buy' or 'sell'

        Kalshi requires EXACTLY ONE of yes_price / no_price even for market
        orders — it's the per-contract ceiling (buy) or floor (sell). We set
        99¢ for buys (effectively unlimited) and 1¢ for sells (accept any
        price). Tighten these if you want stricter slippage protection.
        """
        payload = {
            "ticker":           ticker,
            "side":             side,
            "count":            int(count),
            "type":             order_type,
            "action":           action,
            "client_order_id":  str(uuid.uuid4()),
        }
        # Per-side price field: ceiling for buys, floor for sells
        if action == "buy":
            price = 99
        else:
            price = 1
        if side == "yes":
            payload["yes_price"] = price
        else:
            payload["no_price"]  = price
        return self._post("/portfolio/orders", payload)

    def get_positions(self):
        """Return the user's open market positions."""
        return self._get("/portfolio/positions")

    def get_fills(self, limit: int = 25):
        """Return the user's most recent fills (executed trades).
        Useful for fast-cycling markets like KXBTC15M where positions settle
        within minutes — fills give you a persistent trade history."""
        return self._get("/portfolio/fills", {"limit": limit})


# ─────────────────────────────────────────────────────────────
# DATA STORE
# ─────────────────────────────────────────────────────────────
class DataStore:
    def __init__(self):
        self._lock  = threading.Lock()
        self.btc    = self._empty()
        self.status = {"live": False, "market_btc": "—", "close_time": "—"}

    @staticmethod
    def _empty():
        return dict(
            times=deque(maxlen=MAX_BARS), yes=deque(maxlen=MAX_BARS),
            no=deque(maxlen=MAX_BARS), ob_bids=[], ob_asks=[],
            total_bid=0, total_ask=0, buyer_lvls=0,
            seller_lvls=0, imbalance=0.0, ticker="—",
        )

    def push(self, ts, yp, np, ob_raw):
        with self._lock:
            d = self.btc
            d["times"].append(ts)
            d["yes"].append(yp)
            d["no"].append(np)

            # Find the YES and NO level arrays. Kalshi has shipped a few
            # shapes over time:
            #   { "yes": [[price, qty], ...], "no": [...] }
            #   { "yes_levels": [...], "no_levels": [...] }
            #   { "levels": { "yes": [...], "no": [...] } }
            #   { "yes": [{"price": ..., "size": ...}, ...] }
            def pick_side(side: str):
                for key in (f"{side}_dollars", side, f"{side}_levels",
                            f"{side}_orderbook", f"{side}_levels_dollars"):
                    if key in ob_raw and ob_raw[key]:
                        return ob_raw[key]
                if isinstance(ob_raw.get("levels"), dict):
                    return ob_raw["levels"].get(side) or []
                return []

            yes_side = pick_side("yes")
            no_side  = pick_side("no")

            def to_frac(p):
                try: p = float(p)
                except (TypeError, ValueError): return None
                return p if p <= 1.0 else p / 100

            def to_qty(q):
                try: return float(q)
                except (TypeError, ValueError): return 0.0

            def normalize(level):
                # Each level may be [price, qty] or {"price": .., "size": ..}
                if isinstance(level, dict):
                    p = (level.get("price_dollars") or level.get("price")
                         or level.get("yes_price"))
                    q = (level.get("size_fp") or level.get("size")
                         or level.get("quantity") or level.get("qty"))
                    return to_frac(p), to_qty(q)
                if isinstance(level, (list, tuple)) and len(level) >= 2:
                    return to_frac(level[0]), to_qty(level[1])
                return None, None

            bids_raw = [normalize(l) for l in yes_side]
            asks_raw = [normalize(l) for l in no_side]
            # Bids: sorted by YES price descending — index 0 = best (highest) bid.
            # Asks: sorted by NO  price descending — index 0 = best (lowest) YES ask
            #       (because YES ask = 100 - NO bid, so highest NO bid = lowest YES ask).
            bids = sorted([(p, q) for p, q in bids_raw if p is not None],
                          key=lambda x: -x[0])
            asks = sorted([(p, q) for p, q in asks_raw if p is not None],
                          key=lambda x: -x[0])
            tb   = sum(q for _,q in bids)
            ta   = sum(q for _,q in asks)
            imb  = (tb - ta) / (tb + ta) if (tb + ta) > 0 else 0.0
            d["ob_bids"]     = bids
            d["ob_asks"]     = asks
            d["total_bid"]   = tb
            d["total_ask"]   = ta
            d["buyer_lvls"]  = len(bids)
            d["seller_lvls"] = len(asks)
            d["imbalance"]   = imb
            return {
                "ts": ts, "yes": int(round(yp * 100)), "no": int(round(np * 100)),
                "ob_bids": [[int(round(p*100)), q] for p, q in bids[:20]],
                "ob_asks": [[int(round(p*100)), q] for p, q in asks[:20]],
                "total_bid": tb, "total_ask": ta,
                "buyer_lvls": len(bids), "seller_lvls": len(asks),
                "imbalance": imb,
            }

    def reset(self):
        with self._lock:
            self.btc = self._empty()

    def set_ticker(self, ticker):
        with self._lock:
            self.btc["ticker"] = ticker

    def set_live(self, val):
        with self._lock:
            self.status["live"] = val

    def set_markets(self, bt, close):
        with self._lock:
            self.status.update({"market_btc": bt, "close_time": close})

    def snapshot(self):
        with self._lock:
            d = self.btc
            ticks = []
            ts_l = list(d["times"]); ys_l = list(d["yes"]); ns_l = list(d["no"])
            for i in range(len(ts_l)):
                ticks.append({
                    "ts": ts_l[i],
                    "yes": int(round(ys_l[i] * 100)),
                    "no":  int(round(ns_l[i] * 100)),
                })
            return dict(
                ticker=d["ticker"],
                live=self.status["live"],
                market_btc=self.status["market_btc"],
                close_time=self.status["close_time"],
                trading_enabled=TRADING_ENABLED,
                ticks=ticks,
                ob_bids=[[int(round(p*100)), q] for p, q in d["ob_bids"][:20]],
                ob_asks=[[int(round(p*100)), q] for p, q in d["ob_asks"][:20]],
                total_bid=d["total_bid"], total_ask=d["total_ask"],
                buyer_lvls=d["buyer_lvls"], seller_lvls=d["seller_lvls"],
                imbalance=d["imbalance"],
            )

    def get_ticker(self):
        with self._lock:
            return self.btc["ticker"]


# ─────────────────────────────────────────────────────────────
# CSV LOGGER — saves ticks per ticker + orders rolling
# ─────────────────────────────────────────────────────────────
class CSVLogger:
    """Persists every tick and every order to CSV files in CSV_DIR.

    - Ticks rotate per active ticker. When the 15-min market rolls over,
      a new ticks_{ticker}_{starttime}.csv is opened. This lets you analyze
      one window cleanly without filtering across markets.
    - Orders go into a single orders.csv that grows across sessions —
      every BUY/SELL attempt is appended with the Kalshi response status.
    """

    TICK_COLS = [
        "timestamp_ms", "timestamp_iso", "ticker",
        "yes_cents", "no_cents",
        "yes_bid_cents", "yes_ask_cents", "last_price_cents",
        "total_bid", "total_ask",
        "buyer_lvls", "seller_lvls",
        "imbalance",
        "top_bid_price", "top_bid_qty",
        "top_ask_price", "top_ask_qty",
        "spread_cents",
        "volume",
    ]
    ORDER_COLS = [
        "timestamp_iso", "ticker", "action", "side", "count",
        "ok", "order_id", "status", "error",
    ]

    def __init__(self, log_dir: str):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._tick_file = None
        self._tick_writer = None
        self._tick_ticker = None
        self._orders_path = self.log_dir / "orders.csv"
        if not self._orders_path.exists() or self._orders_path.stat().st_size == 0:
            with self._orders_path.open("w", newline="") as f:
                csv.writer(f).writerow(self.ORDER_COLS)

    def _ensure_tick_file(self, ticker: str):
        if self._tick_ticker == ticker and self._tick_file is not None:
            return
        if self._tick_file is not None:
            self._tick_file.close()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = self.log_dir / f"ticks_{ticker}_{ts}.csv"
        is_new = not path.exists() or path.stat().st_size == 0
        self._tick_file = path.open("a", newline="")
        self._tick_writer = csv.writer(self._tick_file)
        if is_new:
            self._tick_writer.writerow(self.TICK_COLS)
        self._tick_ticker = ticker
        print(f"[csv] ticks → {path}")

    def log_tick(self, row: dict):
        with self._lock:
            self._ensure_tick_file(row["ticker"])
            self._tick_writer.writerow([row.get(c, "") for c in self.TICK_COLS])
            self._tick_file.flush()

    def log_order(self, row: dict):
        with self._lock:
            with self._orders_path.open("a", newline="") as f:
                csv.writer(f).writerow([row.get(c, "") for c in self.ORDER_COLS])

    def close(self):
        with self._lock:
            if self._tick_file is not None:
                self._tick_file.close()
                self._tick_file = None
                self._tick_writer = None


# ─────────────────────────────────────────────────────────────
# SSE broadcaster
# ─────────────────────────────────────────────────────────────
class SSEBroadcaster:
    def __init__(self):
        self._subs: list = []
        self._lock = threading.Lock()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=2000)
        with self._lock:
            self._subs.append(q)
        return q

    def unsubscribe(self, q: queue.Queue):
        with self._lock:
            if q in self._subs:
                self._subs.remove(q)

    def broadcast(self, msg: dict):
        payload = json.dumps(msg)
        with self._lock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(payload)
            except queue.Full:
                pass


# ─────────────────────────────────────────────────────────────
# POLLER
# ─────────────────────────────────────────────────────────────
class BTCSpot:
    """Pulls live BTC/USD spot from Coinbase. Cached for ~500ms so a 4×/sec
    poll loop doesn't hammer the public endpoint."""
    URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    CACHE_S = 0.5

    def __init__(self):
        self._last_fetch = 0.0
        self._last_value = None

    def get(self):
        now = time.time()
        if self._last_value is not None and now - self._last_fetch < self.CACHE_S:
            return self._last_value
        try:
            r = requests.get(self.URL, timeout=2)
            r.raise_for_status()
            self._last_value = float(r.json()["data"]["amount"])
            self._last_fetch = now
        except Exception as e:
            print(f"[btc spot] {e}")
        return self._last_value


class Poller:
    def __init__(self, api: KalshiAPI, store: DataStore,
                 broadcaster: SSEBroadcaster, csv_logger: "CSVLogger" = None):
        self.api        = api
        self.store      = store
        self.broadcaster = broadcaster
        self.csv_logger = csv_logger
        self.btc_market = None
        self.btc_spot   = BTCSpot()
        self._interval  = 1
        self._running   = False

    def start(self, interval=1):
        self._interval = interval
        self._running  = True
        threading.Thread(target=self._loop, daemon=True).start()

    def stop(self):
        self._running = False

    def _loop(self):
        self._refresh()
        while self._running:
            self._poll()
            time.sleep(self._interval)

    def _refresh(self):
        try:
            new_market = self.api.get_active_15m(SERIES)
            new_ticker = new_market["ticker"] if new_market else "—"
            old_ticker = self.btc_market["ticker"] if self.btc_market else None
            self.btc_market = new_market
            close = "—"
            if new_market:
                close = datetime.fromisoformat(
                    new_market["close_time"].replace("Z", "+00:00")
                ).astimezone().strftime("%H:%M:%S")
            # If the active market changed, wipe history and notify clients
            if new_ticker != old_ticker:
                self.store.reset()
                self.store.set_ticker(new_ticker)
                self.store.set_markets(new_ticker, close)
                self.broadcaster.broadcast({
                    "type": "market", "ticker": new_ticker, "close_time": close,
                })
            else:
                self.store.set_markets(new_ticker, close)
            print(f"[markets] BTC={new_ticker}  closes={close}")
        except Exception as e:
            print(f"[refresh] {e}")

    def _poll(self):
        try:
            now = datetime.now(timezone.utc)
            if self.btc_market:
                ct = datetime.fromisoformat(
                    self.btc_market["close_time"].replace("Z","+00:00"))
                if ct < now:
                    self._refresh()

            if not self.btc_market:
                self.store.set_live(False)
                self.broadcaster.broadcast({"type": "status", "live": False})
                return

            ticker   = self.btc_market["ticker"]
            mkt_data = self.api.get_market(ticker)
            ob_raw   = self.api.get_orderbook(ticker)
            ts       = int(time.time() * 1000)

            # One-time diagnostic so we can see exactly which fields Kalshi
            # returns for an authed request (helps confirm RSA auth is working
            # AND tells us the right field names if the schema differs).
            if not getattr(self, "_logged_first", False):
                print(f"[debug] yes_bid_dollars={mkt_data.get('yes_bid_dollars')}  "
                      f"yes_ask_dollars={mkt_data.get('yes_ask_dollars')}  "
                      f"last_price_dollars={mkt_data.get('last_price_dollars')}  "
                      f"volume_fp={mkt_data.get('volume_fp')}")
                print(f"[debug] orderbook full = {json.dumps(ob_raw)[:400]}")
                self._logged_first = True

            yes_cents = self._best_yes_price(mkt_data)
            yp = yes_cents / 100
            np = (100 - yes_cents) / 100   # binary market: YES + NO sum to 100
            tick = self.store.push(ts, yp, np, ob_raw)

            # Live BTC spot + market target (the strike). Letting these flow
            # through the tick payload lets the chart overlay BTC vs target.
            spot   = self.btc_spot.get()
            try:    target = float(mkt_data.get("floor_strike") or 0) or None
            except (TypeError, ValueError): target = None
            tick["btc"]    = spot
            tick["target"] = target

            self.store.set_live(True)
            self.broadcaster.broadcast({"type": "tick", "snap": tick, "live": True})
            spot_str = f"  BTC=${spot:,.2f}" if spot else ""
            tgt_str  = f"  tgt=${target:,.2f}" if target else ""
            print(f"[poll ok] {datetime.now().strftime('%H:%M:%S')}  "
                  f"YES={tick['yes']}c  NO={tick['no']}c{spot_str}{tgt_str}")

            # Persist to CSV — captures everything, not just what the chart shows
            if self.csv_logger is not None:
                top_bid = tick["ob_bids"][0] if tick["ob_bids"] else (None, None)
                top_ask = tick["ob_asks"][0] if tick["ob_asks"] else (None, None)
                spread = None
                if top_bid[0] is not None and top_ask[0] is not None:
                    spread = abs(top_bid[0] - (100 - top_ask[0]))
                def _to_cents(v):
                    if v is None or v == "": return ""
                    try: return int(round(float(v) * 100))
                    except (TypeError, ValueError): return ""
                self.csv_logger.log_tick({
                    "timestamp_ms":      ts,
                    "timestamp_iso":     datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat(),
                    "ticker":            ticker,
                    "yes_cents":         tick["yes"],
                    "no_cents":          tick["no"],
                    "yes_bid_cents":     _to_cents(mkt_data.get("yes_bid_dollars")),
                    "yes_ask_cents":     _to_cents(mkt_data.get("yes_ask_dollars")),
                    "last_price_cents":  _to_cents(mkt_data.get("last_price_dollars")),
                    "total_bid":         tick["total_bid"],
                    "total_ask":         tick["total_ask"],
                    "buyer_lvls":        tick["buyer_lvls"],
                    "seller_lvls":       tick["seller_lvls"],
                    "imbalance":         f'{tick["imbalance"]:.4f}',
                    "top_bid_price":     top_bid[0] if top_bid[0] is not None else "",
                    "top_bid_qty":       top_bid[1] if top_bid[1] is not None else "",
                    "top_ask_price":     top_ask[0] if top_ask[0] is not None else "",
                    "top_ask_qty":       top_ask[1] if top_ask[1] is not None else "",
                    "spread_cents":      spread if spread is not None else "",
                    "volume":            mkt_data.get("volume_fp") or mkt_data.get("volume_24h_fp") or "",
                })
        except Exception as e:
            self.store.set_live(False)
            self.broadcaster.broadcast({"type": "status", "live": False})
            print(f"[poll error] {e}")

    @staticmethod
    def _best_yes_price(mkt: dict) -> float:
        """Pick the freshest YES price. Prefer the live bid/ask midpoint
        (matches what Kalshi's UI shows as the current quote). Fall back to
        last trade only when quotes are missing."""
        def cents(v):
            if v is None or v == "":
                return None
            try:
                return float(v) * 100
            except (TypeError, ValueError):
                return None

        bid  = cents(mkt.get("yes_bid_dollars"))
        ask  = cents(mkt.get("yes_ask_dollars"))
        last = cents(mkt.get("last_price_dollars"))
        if bid and ask and bid > 0 and ask > 0:
            return (bid + ask) / 2
        if ask and ask > 0:
            return ask
        if bid and bid > 0:
            return bid
        if last and last > 0:
            return last
        return 50.0


# ─────────────────────────────────────────────────────────────
# HTML / JS FRONTEND  (v2.1-style time-axis chart with YES + NO lines)
# ─────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Kalshi BTC 15M — Live</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Syne:wght@700;800&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#0a0c0f; --surface:#111318; --surface2:#181c23;
  --border:#1e2330; --border2:#252c3a;
  --green:#00e5a0; --green-dim:rgba(0,229,160,0.13);
  --red:#ff4d6d;   --red-dim:rgba(255,77,109,0.13);
  --amber:#f5a623;
  --orange:#ff8c42; --orange-dim:rgba(255,140,66,0.13);
  --text:#e8eaf0;  --muted:#5a6070;
  --font:'DM Mono',monospace; --display:'Syne',sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--font);
  height:100vh;display:flex;flex-direction:column;overflow:hidden;user-select:none}
.header{display:flex;align-items:center;gap:18px;flex-wrap:wrap;
  padding:10px 18px;border-bottom:1px solid var(--border);background:var(--surface);flex-shrink:0}
.logo{font-family:var(--display);font-size:16px;font-weight:800;letter-spacing:0.5px}
.logo .ax{color:var(--orange)} .logo .dim{color:var(--muted);margin:0 5px}
.meta{font-size:11px;color:var(--muted);display:flex;gap:14px;flex-wrap:wrap}
.meta b{color:var(--text);font-weight:500}
.live-indicator{display:flex;align-items:center;gap:6px;font-size:10px;color:var(--muted)}
.live-dot{width:7px;height:7px;border-radius:50%;background:var(--red)}
.live-dot.connected{background:var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}
.toolbar{display:flex;align-items:center;gap:6px;flex-wrap:wrap;
  padding:6px 16px;border-bottom:1px solid var(--border);background:var(--surface);flex-shrink:0}
.tb-btn{padding:5px 11px;border-radius:5px;font-family:var(--font);
  font-size:11px;cursor:pointer;border:1px solid var(--border2);
  background:var(--surface2);color:var(--muted);transition:all .12s}
.tb-btn:hover{border-color:var(--green);color:var(--green)}
.tb-btn.active{background:var(--green-dim);border-color:var(--green);color:var(--green)}
.tb-sel{padding:5px 8px;border-radius:5px;font-family:var(--font);font-size:11px;
  cursor:pointer;border:1px solid var(--border2);background:var(--surface2);
  color:var(--muted);outline:none}
.tb-sel:hover{border-color:var(--orange);color:var(--orange)}
.tb-sel.locked{border-color:var(--orange);color:var(--orange);background:var(--orange-dim)}
.zoom-info{margin-left:auto;font-size:10px;color:var(--muted)}
.body{flex:1;display:flex;overflow:hidden;min-height:0}
.chart-wrap{flex:1;display:flex;flex-direction:column;min-width:0}
.panel{position:relative;flex:1;overflow:hidden}
.panel-data-bar{position:absolute;top:6px;left:14px;right:78px;z-index:5;
  display:flex;align-items:center;gap:11px;pointer-events:none;
  font-size:10.5px;line-height:1;flex-wrap:wrap}
.pdb-name{font-family:var(--display);font-weight:700;font-size:12px;
  padding:2px 6px;border-radius:3px;color:var(--orange);background:var(--orange-dim)}
.pdb-time{color:var(--muted);font-size:10px}
.pdb-item{display:flex;align-items:center;gap:4px}
.pdb-label{color:var(--muted);font-size:10px}
.pdb-val{font-weight:500;font-size:11px}
.pdb-val.g{color:var(--green)} .pdb-val.r{color:var(--red)} .pdb-val.w{color:var(--text)}
canvas{display:block;cursor:crosshair;width:100%;height:100%}
.price-tag{position:absolute;right:6px;padding:3px 7px;border-radius:3px;
  font-size:10px;font-weight:500;pointer-events:none;z-index:6;
  background:var(--surface);border:1px solid var(--border2)}
.price-tag.yes{color:var(--green);border-color:var(--green)}
.price-tag.no{color:var(--red);border-color:var(--red)}

/* Live BTC spot HUD — top-right of chart, mirrors KXBTC15M HUD on top-left */
.btc-hud{position:absolute;top:6px;right:80px;z-index:5;
  display:flex;align-items:center;gap:6px;
  pointer-events:none;font-family:var(--font)}
.btc-hud .label{font-family:var(--display);font-weight:700;font-size:12px;
  letter-spacing:0.5px;color:var(--orange);
  background:var(--orange-dim);padding:2px 6px;border-radius:3px}
.btc-hud .price{color:var(--orange);font-weight:600;font-size:13px}
.btc-hud .diff{font-size:10px;color:var(--muted)}
.btc-hud .diff.pos{color:var(--green)}
.btc-hud .diff.neg{color:var(--red)}
.tooltip{position:fixed;pointer-events:none;z-index:10;
  background:var(--surface);border:1px solid var(--border2);
  padding:9px 12px;font-size:10.5px;border-radius:4px;
  min-width:200px;line-height:1.55;
  box-shadow:0 4px 14px rgba(0,0,0,0.55)}
.tt-row{display:flex;justify-content:space-between;gap:14px}
.tt-label{color:var(--muted)}
.tt-val.g{color:var(--green)} .tt-val.r{color:var(--red)}
.tt-divider{height:1px;background:var(--border2);margin:5px 0}
.scrollbar{height:30px;background:var(--surface);border-top:1px solid var(--border);
  flex-shrink:0;padding:6px 14px}
.sb-track{height:100%;background:var(--surface2);border-radius:3px;
  position:relative;cursor:pointer;border:1px solid var(--border2)}
.sb-thumb{position:absolute;top:0;bottom:0;background:var(--green-dim);
  border:1px solid var(--green);border-radius:3px;cursor:grab;min-width:30px}
.sb-thumb.dragging{cursor:grabbing;background:rgba(0,229,160,0.25)}

/* Depth / S-D liquidity indicator */
.depth-bar{flex-shrink:0;background:var(--surface);border-top:1px solid var(--border);
  padding:6px 14px;display:flex;flex-direction:column;gap:3px}
.dr{display:flex;align-items:center;gap:10px}
.dbuy{color:var(--green);font-size:10px;min-width:170px;font-weight:500}
.dsell{color:var(--red);font-size:10px;min-width:170px;text-align:right;font-weight:500}
.iw{flex:1;height:10px;position:relative}
.iw canvas{width:100%;height:100%;display:block}
.spr{font-size:9px;color:var(--muted);text-align:center}

/* Trade row */
.trade-row{flex-shrink:0;background:var(--surface);border-top:1px solid var(--border);
  padding:8px 14px;display:flex;align-items:center;gap:8px}
.trade-row label{font-size:10px;color:var(--muted)}
.trade-row input{width:60px;background:var(--surface2);border:1px solid var(--border2);
  color:var(--text);font-family:var(--font);font-size:11px;
  padding:4px 8px;border-radius:4px;text-align:center}
.tbuy,.tno{padding:5px 14px;border-radius:5px;font-family:var(--font);font-size:11px;
  font-weight:600;cursor:pointer;border:none}
.tbuy{background:var(--green);color:#0a0c0f}
.tno{background:var(--red);color:#0a0c0f}
.tbuy:hover,.tno:hover{opacity:0.85}
.tnote{font-size:9px;color:var(--muted);margin-left:auto}
/* Flow indicator — its own dedicated row above the chart so changing text
   width doesn't shift the toolbar or any other element */
.flow-row{flex-shrink:0;background:var(--surface2);
  border-bottom:1px solid var(--border);
  padding:5px 18px;font-size:10px;font-family:var(--font);
  color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
  height:24px;display:flex;align-items:center}
.flow-bid{color:var(--green)} .flow-ask{color:var(--red)}
.flow-verdict{font-weight:600;margin-left:6px}
.flow-verdict.bull{color:var(--green)}
.flow-verdict.bear{color:var(--red)}
.flow-verdict.neutral{color:var(--amber)}

/* Positions panel */
.positions{flex-shrink:0;background:#0d1014;border-top:1px solid var(--border);
  padding:6px 14px;max-height:200px;overflow-y:auto}
.fills-header{font-family:var(--display);font-weight:700;font-size:10px;
  color:var(--muted);letter-spacing:0.5px;margin-top:8px;margin-bottom:4px;
  padding-top:6px;border-top:1px solid var(--border2)}
.fill-row{display:flex;align-items:center;gap:10px;padding:3px 0;font-size:10px;
  color:var(--text)}
.fill-time{color:var(--muted);min-width:64px;font-size:9px}
.fill-action{font-weight:600;min-width:40px;font-size:10px}
.fill-action.buy{color:var(--green)} .fill-action.sell{color:var(--amber)}
.fill-side{font-weight:600;min-width:30px}
.fill-side.yes{color:var(--green)} .fill-side.no{color:var(--red)}
.fill-ticker{color:var(--muted);font-size:9px;flex:1;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.fill-qty,.fill-price{color:var(--text);min-width:50px;text-align:right}
.pos-header{font-family:var(--display);font-weight:700;font-size:10px;
  color:var(--muted);letter-spacing:0.5px;margin-bottom:4px;display:flex;
  justify-content:space-between;align-items:center}
.pos-empty{color:var(--muted);font-size:10px;padding:4px 0;font-style:italic}
.pos-row{display:flex;align-items:center;gap:10px;padding:4px 0;font-size:11px;
  border-bottom:1px dashed var(--border2)}
.pos-row:last-child{border-bottom:none}
.pos-ticker{color:var(--muted);font-size:10px;min-width:230px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.pos-ticker.active{color:var(--orange);font-weight:500}
.pos-side{font-weight:700;min-width:60px;font-size:10px;letter-spacing:0.5px}
.pos-side.yes{color:var(--green)} .pos-side.no{color:var(--red)}
.pos-side.neutral{color:var(--amber)}
.pos-flat-badge{margin-left:auto;padding:4px 14px;border-radius:4px;
  background:var(--amber-dim);color:var(--amber);font-weight:700;
  font-size:10px;letter-spacing:0.5px}
.pos-qty{color:var(--text);min-width:90px}
.pos-cur{color:var(--muted);min-width:62px}
.pos-pnl{font-weight:600;min-width:70px}
.pos-pnl.pos{color:var(--green)} .pos-pnl.neg{color:var(--red)}
.pos-sell{margin-left:auto;padding:4px 14px;border-radius:4px;
  background:var(--red);color:#0a0c0f;border:none;cursor:pointer;
  font-weight:700;font-size:10px;font-family:var(--font);letter-spacing:0.5px}
.pos-sell:hover{opacity:0.85}
.ob{width:200px;flex-shrink:0;background:#090b0e;
  border-left:1px solid var(--border);display:flex;flex-direction:column}
.ob-h{height:28px;background:var(--surface);border-bottom:1px solid var(--border);
  display:flex;align-items:center;padding:0 10px;font-size:11px;
  font-family:var(--display);font-weight:700;letter-spacing:0.5px;color:var(--orange)}
.ob-c{display:flex;font-size:9px;color:var(--muted);padding:4px 6px;flex-shrink:0;
  border-bottom:1px solid var(--border)}
.ob-c span{flex:1}
.ob-canvas{flex:1}
</style>
</head>
<body>

<div class="header">
  <div class="logo"><span class="ax">KALSHI</span><span class="dim">·</span>BTC 15M</div>
  <div class="meta">
    <span>Market <b id="hd-ticker">—</b></span>
    <span>Closes <b id="hd-close">—</b></span>
    <span>Ticks <b id="hd-ticks">0</b></span>
    <span>YES <b id="hd-yes">—</b></span>
    <span>NO <b id="hd-no">—</b></span>
  </div>
  <div class="live-indicator"><span class="live-dot" id="live-dot"></span><span id="live-text">Connecting…</span></div>
</div>

<div class="toolbar">
  <button class="tb-btn" id="btn-50">50% line</button>
  <button class="tb-btn" id="btn-y0100">Y 0–100</button>
  <button class="tb-btn" id="btn-reset">Reset zoom</button>
  <button class="tb-btn active" id="btn-autoscroll">Full view</button>
  <select class="tb-sel" id="btc-range-sel" title="Lock BTC y-axis to ±X% of target">
    <option value="auto" selected>BTC: Auto</option>
    <option value="0.001">BTC: ±0.100%</option>
    <option value="0.002">BTC: ±0.200%</option>
    <option value="0.003">BTC: ±0.300%</option>
    <option value="0.004">BTC: ±0.400%</option>
    <option value="0.005">BTC: ±0.500%</option>
  </select>
  <button class="tb-btn" id="btn-target-center" title="Pin the target price to the vertical center of the BTC axis">Target center</button>
</div>

<div class="flow-row"><span id="flow-ind">flow: —</span></div>

<div class="body">
  <div class="chart-wrap">
    <div class="panel" id="panel-main">
      <div class="panel-data-bar" id="data-bar"></div>
      <div class="btc-hud" id="btc-hud"></div>
      <canvas id="chart-canvas"></canvas>
      <div class="price-tag yes" id="tag-yes" style="display:none"></div>
      <div class="price-tag no"  id="tag-no"  style="display:none"></div>
    </div>
    <div class="depth-bar">
      <div class="dr">
        <div class="dbuy" id="dbuy">BUYERS — lvl / — ct</div>
        <div class="iw"><canvas id="imb-canvas"></canvas></div>
        <div class="dsell" id="dsell">SELLERS — lvl / — ct</div>
      </div>
      <div class="spr" id="spr">spread —</div>
    </div>
    <div class="trade-row">
      <label>Qty</label>
      <input type="number" id="trade-qty" value="1" min="1">
      <button class="tbuy" onclick="trade('yes')">BUY YES</button>
      <button class="tno"  onclick="trade('no')">BUY NO</button>
      <span class="tnote" id="trade-note">[trading disabled]</span>
    </div>
    <div class="positions">
      <div class="pos-header"><span>POSITIONS · LIVE P&L</span><span id="pos-status"></span></div>
      <div id="pos-list"><div class="pos-empty">No open positions across portfolio</div></div>
      <div class="fills-header">RECENT FILLS</div>
      <div id="fill-list"><div class="pos-empty">No recent fills</div></div>
    </div>
    <div class="scrollbar"><div class="sb-track" id="sb-track"><div class="sb-thumb" id="sb-thumb"></div></div></div>
  </div>
  <div class="ob">
    <div class="ob-h">ORDERBOOK</div>
    <div class="ob-c"><span>PRICE</span><span>QTY</span><span>SIDE</span></div>
    <canvas class="ob-canvas" id="ob-canvas"></canvas>
  </div>
</div>

<div class="tooltip" id="tt" style="display:none"></div>

<script>
let TICKS = [];                 // [{ts, yes, no}, ...]
let OB = {bids: [], asks: []};
let DEPTH = {buyer_lvls:0, seller_lvls:0, total_bid:0, total_ask:0, imbalance:0};
let TICKER = '—', CLOSE = '—';
let TARGET = null;              // BTC strike for the active market (dollars)
let POSITIONS = [];
let FILLS = [];
let PRICES = {};                // ticker → {yes, no} cents, for non-active markets

// 90¢ alert: line drawn on chart, distinct chime tones for YES@90 vs NO@90.
const ALERT_THRESHOLD = 90;
let yesAlertArmed = true;       // re-arms when YES drops 2¢ below threshold
let noAlertArmed  = true;       // independent — NO@90 == YES@10
let audioCtx = null;
// Flow window for the supply/demand pressure indicator (tracks total_bid /
// total_ask over the last N seconds so we can show rate of change).
const FLOW_WINDOW_MS = 5000;
let flowSamples = [];           // [{t, total_bid, total_ask}, ...]

let viewTMin = null, viewTMax = null;
let yState = {min: 0, max: 100, manual: false};
let show50 = false, autoscroll = true, yPinned = false;
let crosshairT = null;
// BTC y-axis lock: 'auto' = scale to data; otherwise a fraction (0.001 = ±0.1%)
// applied as a band centered on the target price. Set via the toolbar dropdown.
let btcRangeMode = 'auto';
// When true, force the target price to sit at the exact vertical center of
// the BTC axis. Independent of the dropdown — works even in Auto mode.
let targetCentered = false;

const canvas = document.getElementById('chart-canvas');
const obCanvas = document.getElementById('ob-canvas');

function getPad(){ return {left:50, right:60, top:30, bottom:24}; }
function pw(W,p){ return W - p.left - p.right; }
function ph(H,p){ return H - p.top - p.bottom; }

function fullTimeRange(){
  if (!TICKS.length) return null;
  const tmin = TICKS[0].ts, tmax = TICKS[TICKS.length-1].ts;
  if (tmin === tmax) return [tmin, tmin + 1000];
  return [tmin, tmax];
}

function clampTimeView(){
  const r = fullTimeRange();
  if (!r) return;
  if (viewTMin == null || viewTMax == null){ [viewTMin, viewTMax] = r; return; }
  const minSpan = 5000;
  if (viewTMax - viewTMin < minSpan) viewTMax = viewTMin + minSpan;
}

function snapAtTime(arr, tMs){
  if (!arr.length) return null;
  let lo = 0, hi = arr.length - 1;
  if (tMs <= arr[0].ts) return arr[0];
  if (tMs >= arr[hi].ts) return arr[hi];
  while (lo < hi){
    const mid = (lo + hi + 1) >> 1;
    if (arr[mid].ts <= tMs) lo = mid; else hi = mid - 1;
  }
  return arr[lo];
}

function getY(){
  if (yState.manual) return {yMin: yState.min, yMax: yState.max};
  const visible = TICKS.filter(t => t.ts >= viewTMin && t.ts <= viewTMax);
  if (!visible.length) return {yMin: 0, yMax: 100};
  const vals = visible.flatMap(t => [t.yes, t.no]);
  let lo = Math.min(...vals), hi = Math.max(...vals);
  const r = Math.max(2, hi - lo);
  return {yMin: Math.max(0, lo - r * 0.18), yMax: Math.min(100, hi + r * 0.18)};
}

function fmtTime(t){
  const d = new Date(t);
  return ('0'+d.getHours()).slice(-2)+':'+('0'+d.getMinutes()).slice(-2)+':'+('0'+d.getSeconds()).slice(-2);
}

function drawChart(){
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.offsetWidth, H = canvas.offsetHeight;
  canvas.width = W * dpr; canvas.height = H * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);

  const pad = getPad();
  if (viewTMin == null || viewTMax == null || viewTMax <= viewTMin) return;
  const {yMin, yMax} = getY();
  const tSpan = viewTMax - viewTMin;
  const xOfT = t => pad.left + ((t - viewTMin) / tSpan) * pw(W, pad);
  const yOfP = p => pad.top + ((yMax - p) / (yMax - yMin)) * ph(H, pad);

  // Compute BTC y-range for the secondary right axis.
  //   • Locked (dropdown ≠ Auto): fixed band centered on target.
  //   • Auto + targetCentered:    auto-fit, then expand symmetrically so
  //                               target sits at exact midpoint.
  //   • Auto:                     simple fit to visible data + target.
  function btcRange(){
    if (btcRangeMode !== 'auto' && TARGET){
      const half = TARGET * btcRangeMode;
      return [TARGET - half, TARGET + half];
    }
    const vals = TICKS
      .filter(t => t.btc != null && t.ts >= viewTMin && t.ts <= viewTMax)
      .map(t => t.btc);
    if (!vals.length) return null;
    let lo = Math.min(...vals), hi = Math.max(...vals);
    if (TARGET){ lo = Math.min(lo, TARGET); hi = Math.max(hi, TARGET); }
    if (targetCentered && TARGET){
      // Whichever side is farther from target sets the half-width.
      const half = Math.max(TARGET - lo, hi - TARGET) * 1.15;
      return [TARGET - half, TARGET + half];
    }
    const r = Math.max(1, hi - lo);
    return [lo - r * 0.15, hi + r * 0.15];
  }
  const btcR = btcRange();
  const yOfBtc = (v) => btcR
    ? pad.top + ((btcR[1] - v) / (btcR[1] - btcR[0])) * ph(H, pad)
    : null;

  // Grid + LEFT axis (cents 0-100 for YES/NO) and RIGHT axis ($ for BTC)
  ctx.strokeStyle = 'rgba(80,90,110,0.18)';
  ctx.lineWidth = 1;
  ctx.fillStyle = '#5a6070';
  ctx.font = '10px DM Mono, monospace';
  ctx.textBaseline = 'middle';
  for (let i = 0; i <= 4; i++){
    const y = pad.top + (i / 4) * ph(H, pad);
    const cents = yMax - (i / 4) * (yMax - yMin);
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(W - pad.right, y); ctx.stroke();
    ctx.textAlign = 'right';
    ctx.fillText(cents.toFixed(0) + 'c', pad.left - 6, y);
    ctx.textAlign = 'left';
    if (btcR){
      const dollars = btcR[1] - (i / 4) * (btcR[1] - btcR[0]);
      ctx.fillStyle = '#ff8c42';
      ctx.fillText('$' + Math.round(dollars).toLocaleString(), W - pad.right + 6, y);
      ctx.fillStyle = '#5a6070';
    } else {
      ctx.fillText(cents.toFixed(0) + 'c', W - pad.right + 6, y);
    }
  }

  if (show50 && yMin <= 50 && yMax >= 50){
    const y50 = yOfP(50);
    ctx.strokeStyle = 'rgba(245,166,35,0.5)';
    ctx.setLineDash([4, 4]);
    ctx.beginPath(); ctx.moveTo(pad.left, y50); ctx.lineTo(W - pad.right, y50); ctx.stroke();
    ctx.setLineDash([]);
  }

  // 90¢ alert line — drawn whenever it falls inside the current y-range
  if (yMin <= ALERT_THRESHOLD && yMax >= ALERT_THRESHOLD){
    const yA = yOfP(ALERT_THRESHOLD);
    ctx.strokeStyle = 'rgba(0, 229, 160, 0.55)';
    ctx.setLineDash([6, 4]);
    ctx.beginPath(); ctx.moveTo(pad.left, yA); ctx.lineTo(W - pad.right, yA); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = '#00e5a0';
    ctx.font = '10px DM Mono, monospace';
    ctx.textAlign = 'left'; ctx.textBaseline = 'bottom';
    ctx.fillText(`${ALERT_THRESHOLD}¢ alert`, pad.left + 6, yA - 2);
  }

  if (TICKS.length){
    for (const [field, color] of [['no', '#ff4d6d'], ['yes', '#00e5a0']]){
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.6;
      ctx.beginPath();
      let started = false;
      for (let i = 0; i < TICKS.length; i++){
        const pt = TICKS[i];
        if (pt.ts < viewTMin) continue;
        if (pt.ts > viewTMax) break;
        const x = xOfT(pt.ts);
        const y = yOfP(pt[field]);
        if (!started){ ctx.moveTo(x, y); started = true; } else { ctx.lineTo(x, y); }
      }
      ctx.stroke();
    }

    // Target line — horizontal dashed at the strike price, on BTC's $ axis
    if (btcR && TARGET && TARGET >= btcR[0] && TARGET <= btcR[1]){
      const yT = yOfBtc(TARGET);
      ctx.strokeStyle = 'rgba(255, 140, 66, 0.6)';
      ctx.setLineDash([6, 4]);
      ctx.beginPath();
      ctx.moveTo(pad.left, yT); ctx.lineTo(W - pad.right, yT);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#ff8c42';
      ctx.font = '10px DM Mono, monospace';
      ctx.textAlign = 'left'; ctx.textBaseline = 'bottom';
      ctx.fillText(`target $${TARGET.toLocaleString()}`, pad.left + 6, yT - 2);
    }

    // BTC spot price line — orange, on BTC's $ axis
    if (btcR){
      ctx.strokeStyle = '#ff8c42';
      ctx.lineWidth = 1.4;
      ctx.beginPath();
      let started = false;
      let firstBtc = null, lastBtc = null;
      for (const pt of TICKS){
        if (pt.btc == null) continue;
        if (pt.ts < viewTMin) continue;
        if (pt.ts > viewTMax) break;
        if (firstBtc == null) firstBtc = pt;
        lastBtc = pt;
        const x = xOfT(pt.ts);
        const y = yOfBtc(pt.btc);
        if (!started){ ctx.moveTo(x, y); started = true; }
        else { ctx.lineTo(x, y); }
      }
      ctx.stroke();

      // Kalshi-style pill: % distance from the TARGET price.
      // Positive (green) → BTC above target, YES is favored.
      // Negative (red)   → BTC below target, NO  is favored.
      if (lastBtc && TARGET && TARGET > 0){
        const pct = ((lastBtc.btc - TARGET) / TARGET) * 100;
        const positive = pct >= 0;
        const text = `${positive ? '+' : ''}${pct.toFixed(3)}%`;
        const dotX = xOfT(lastBtc.ts);
        const dotY = yOfBtc(lastBtc.btc);

        // Marker circle at the BTC line tip
        ctx.fillStyle = '#ff8c42';
        ctx.strokeStyle = '#0a0c0f'; ctx.lineWidth = 2;
        ctx.beginPath(); ctx.arc(dotX, dotY, 4.5, 0, Math.PI * 2); ctx.fill(); ctx.stroke();

        // Pill badge to the right of the dot (clamped inside chart area)
        ctx.font = 'bold 10px DM Mono, monospace';
        const padX = 10, padY = 5, gap = 8;
        const tw   = ctx.measureText(text).width;
        const bw   = tw + padX * 2;
        const bh   = 18;
        let bx = dotX + gap;
        let by = dotY - bh / 2;
        if (bx + bw > W - pad.right - 2) bx = dotX - gap - bw;       // flip left if no room
        if (by < pad.top)                 by = pad.top + 2;
        if (by + bh > H - pad.bottom)     by = H - pad.bottom - bh - 2;

        const fill   = positive ? 'rgba(0, 229, 160, 0.20)' : 'rgba(255, 77, 109, 0.20)';
        const stroke = positive ? 'rgba(0, 229, 160, 0.85)' : 'rgba(255, 77, 109, 0.85)';
        const txt    = positive ? '#00e5a0' : '#ff4d6d';

        // Pill (rounded rect via arcs — works on every browser)
        const r = bh / 2;
        ctx.fillStyle = fill; ctx.strokeStyle = stroke; ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(bx + r, by);
        ctx.arcTo(bx + bw, by,        bx + bw, by + r,      r);
        ctx.arcTo(bx + bw, by + bh,   bx + bw - r, by + bh, r);
        ctx.arcTo(bx,      by + bh,   bx, by + bh - r,      r);
        ctx.arcTo(bx,      by,        bx + r, by,           r);
        ctx.closePath();
        ctx.fill(); ctx.stroke();

        ctx.fillStyle = txt;
        ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        ctx.fillText(text, bx + bw / 2, by + bh / 2);
      }
    }
  }

  ctx.fillStyle = '#5a6070'; ctx.textAlign = 'center'; ctx.textBaseline = 'top';
  for (let i = 0; i <= 5; i++){
    const t = viewTMin + (i / 5) * tSpan;
    const x = pad.left + (i / 5) * pw(W, pad);
    ctx.fillText(fmtTime(t), x, H - pad.bottom + 4);
  }

  if (crosshairT !== null && crosshairT >= viewTMin && crosshairT <= viewTMax){
    const hoverPt = snapAtTime(TICKS, crosshairT);
    const hx = xOfT(crosshairT);
    ctx.strokeStyle = 'rgba(200,210,230,0.35)';
    ctx.setLineDash([3, 3]);
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(hx, pad.top); ctx.lineTo(hx, H - pad.bottom); ctx.stroke();
    ctx.setLineDash([]);

    if (hoverPt){
      const yY = yOfP(hoverPt.yes);
      ctx.fillStyle = '#00e5a0';
      ctx.beginPath(); ctx.arc(hx, yY, 3.5, 0, Math.PI*2); ctx.fill();
      ctx.strokeStyle = '#0a0c0f'; ctx.lineWidth = 1.5; ctx.stroke();
      const yN = yOfP(hoverPt.no);
      ctx.fillStyle = '#ff4d6d';
      ctx.beginPath(); ctx.arc(hx, yN, 3.5, 0, Math.PI*2); ctx.fill();
      ctx.strokeStyle = '#0a0c0f'; ctx.lineWidth = 1.5; ctx.stroke();

      ctx.fillStyle = '#00e5a0';
      ctx.fillRect(W - pad.right + 1, yY - 8, pad.right - 4, 16);
      ctx.fillStyle = '#0a0c0f'; ctx.font = '10px DM Mono, monospace';
      ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
      ctx.fillText(hoverPt.yes + 'c', W - pad.right + (pad.right - 4)/2 + 1, yY);
      ctx.fillStyle = '#ff4d6d';
      ctx.fillRect(W - pad.right + 1, yN - 8, pad.right - 4, 16);
      ctx.fillStyle = '#0a0c0f';
      ctx.fillText(hoverPt.no + 'c', W - pad.right + (pad.right - 4)/2 + 1, yN);
    }

    ctx.fillStyle = 'rgba(200,210,230,0.9)';
    const ts = fmtTime(crosshairT);
    ctx.font = '10px DM Mono, monospace';
    ctx.textAlign = 'center'; ctx.textBaseline = 'top';
    const tw = ctx.measureText(ts).width + 10;
    ctx.fillRect(hx - tw/2, H - pad.bottom + 2, tw, 14);
    ctx.fillStyle = '#0a0c0f';
    ctx.fillText(ts, hx, H - pad.bottom + 4);
  }

  if (TICKS.length){
    const last = TICKS[TICKS.length - 1];
    const tagY = document.getElementById('tag-yes');
    const tagN = document.getElementById('tag-no');
    if (crosshairT !== null){ tagY.style.display = 'none'; tagN.style.display = 'none'; }
    else {
      tagY.textContent = last.yes + 'c'; tagY.style.top = (yOfP(last.yes) - 9) + 'px'; tagY.style.display = 'block';
      tagN.textContent = last.no  + 'c'; tagN.style.top = (yOfP(last.no)  - 9) + 'px'; tagN.style.display = 'block';
    }
  }

  updateDataBar();
}

function updateDataBar(){
  const last = TICKS.length ? TICKS[TICKS.length - 1] : null;
  const bar = document.getElementById('data-bar');
  if (!last){ bar.innerHTML = ''; return; }
  bar.innerHTML = `
    <span class="pdb-name">KXBTC15M</span>
    <span class="pdb-time">${fmtTime(last.ts)}</span>
    <div class="pdb-item"><span class="pdb-label">YES</span><span class="pdb-val g">${last.yes}c</span></div>
    <div class="pdb-item"><span class="pdb-label">NO</span><span class="pdb-val r">${last.no}c</span></div>
  `;
  document.getElementById('hd-yes').textContent = last.yes + 'c';
  document.getElementById('hd-no').textContent  = last.no  + 'c';
  document.getElementById('hd-ticks').textContent = TICKS.length;

  // Top-right HUD: live BTC spot from Coinbase + $ delta vs target
  const hud = document.getElementById('btc-hud');
  if (last.btc != null){
    const priceFmt = `$${last.btc.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    let diffHtml = '';
    if (TARGET){
      const d = last.btc - TARGET;
      const cls = d >= 0 ? 'pos' : 'neg';
      diffHtml = `<span class="diff ${cls}">${d >= 0 ? '+' : ''}$${Math.abs(d).toFixed(2)}</span>`;
    }
    hud.innerHTML = `<span class="label">BTC</span><span class="price">${priceFmt}</span>${diffHtml}`;
  } else {
    hud.innerHTML = `<span class="label">BTC</span><span class="diff">—</span>`;
  }
}

function drawOrderbook(){
  const dpr = window.devicePixelRatio || 1;
  const W = obCanvas.offsetWidth, H = obCanvas.offsetHeight;
  obCanvas.width = W * dpr; obCanvas.height = H * dpr;
  const ctx = obCanvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);
  ctx.fillStyle = '#090b0e'; ctx.fillRect(0, 0, W, H);

  const bids = OB.bids.slice(0, 16), asks = OB.asks.slice(0, 16);
  if (!bids.length && !asks.length){
    ctx.fillStyle = '#5a6070'; ctx.font = '10px DM Mono, monospace';
    ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    ctx.fillText('Waiting for book…', W/2, H/2);
    return;
  }
  const allQ = [...bids, ...asks].map(x => x[1]);
  const maxQ = allQ.length ? Math.max(...allQ) : 1;
  const rowH = Math.max(Math.floor(H / (bids.length + asks.length + 1)), 12);
  const bw = Math.max(W - 100, 30);
  const midY = Math.floor(H / 2);
  ctx.font = '10px DM Mono, monospace';

  // asks come best-first from the backend (highest NO bid = lowest YES ask).
  // Display the best ask just above the mid line; worst-of-top-20 furthest up.
  asks.forEach(([p, q], i) => {
    const y = midY - (i + 1) * rowH;
    if (y < 0) return;
    const dw = Math.max(1, Math.round((q / maxQ) * bw));
    ctx.fillStyle = 'rgba(60,15,25,0.9)'; ctx.fillRect(W - dw - 4, y, dw, rowH - 2);
    ctx.fillStyle = '#ff4d6d'; ctx.textAlign = 'left';
    ctx.fillText((100 - p) + 'c', 4, y + rowH - 4);
    ctx.fillStyle = '#e8eaf0'; ctx.fillText(q.toString(), 50, y + rowH - 4);
    ctx.fillStyle = '#ff4d6d'; ctx.fillText('SELL', 100, y + rowH - 4);
  });

  ctx.strokeStyle = '#1e2330'; ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(0, midY); ctx.lineTo(W, midY); ctx.stroke();
  if (bids.length && asks.length){
    const sp = Math.abs(bids[0][0] - (100 - asks[0][0]));
    ctx.fillStyle = '#5a6070'; ctx.textAlign = 'center';
    ctx.fillText('spd ' + sp + 'c', W / 2, midY + 8);
  }

  bids.forEach(([p, q], i) => {
    const y = midY + i * rowH + 2;
    if (y + rowH > H) return;
    const dw = Math.max(1, Math.round((q / maxQ) * bw));
    ctx.fillStyle = 'rgba(10,46,30,0.9)'; ctx.fillRect(W - dw - 4, y, dw, rowH - 2);
    ctx.fillStyle = '#00e5a0'; ctx.textAlign = 'left';
    ctx.fillText(p + 'c', 4, y + rowH - 4);
    ctx.fillStyle = '#e8eaf0'; ctx.fillText(q.toString(), 50, y + rowH - 4);
    ctx.fillStyle = '#00e5a0'; ctx.fillText('BUY', 100, y + rowH - 4);
  });
}

function updateScrollbar(){
  const thumb = document.getElementById('sb-thumb');
  const track = document.getElementById('sb-track');
  const tw = track.offsetWidth;
  const range = fullTimeRange();
  if (!range || viewTMin == null){ thumb.style.width = '0'; return; }
  const [absMin, absMax] = range;
  const absSpan = Math.max(1, absMax - absMin);
  const viewSpan = Math.max(1, viewTMax - viewTMin);
  thumb.style.left = ((viewTMin - absMin) / absSpan) * tw + 'px';
  thumb.style.width = Math.max(30, (viewSpan / absSpan) * tw) + 'px';
}

function drawDepth(){
  document.getElementById('dbuy').textContent  = `BUYERS  ${DEPTH.buyer_lvls} lvl / ${Math.round(DEPTH.total_bid)} ct`;
  document.getElementById('dsell').textContent = `SELLERS ${DEPTH.seller_lvls} lvl / ${Math.round(DEPTH.total_ask)} ct`;

  // Spread from top-of-book if we have it
  let sprText = 'spread —';
  if (OB.bids.length && OB.asks.length){
    const yesAsk = 100 - OB.asks[0][0];
    const yesBid = OB.bids[0][0];
    sprText = `spread ${Math.abs(yesAsk - yesBid)}c`;
  }
  document.getElementById('spr').textContent = sprText;

  // Imbalance bar
  const cv = document.getElementById('imb-canvas');
  const dpr = window.devicePixelRatio || 1;
  const W = cv.offsetWidth || 200, H = cv.offsetHeight || 10;
  cv.width = W * dpr; cv.height = H * dpr;
  const ctx = cv.getContext('2d'); ctx.scale(dpr, dpr);
  ctx.fillStyle = '#232823'; ctx.fillRect(0, 1, W, H - 2);
  const mid = W / 2;
  const imb = DEPTH.imbalance || 0;
  const fw = Math.abs(imb) * mid;
  ctx.fillStyle = imb >= 0 ? '#00e5a0' : '#ff4d6d';
  imb >= 0 ? ctx.fillRect(mid, 1, fw, H - 2) : ctx.fillRect(mid - fw, 1, fw, H - 2);
  ctx.strokeStyle = '#323832'; ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(mid, 0); ctx.lineTo(mid, H); ctx.stroke();
  ctx.fillStyle = '#e8eaf0'; ctx.font = '8px DM Mono, monospace'; ctx.textAlign = 'center';
  ctx.fillText(`${imb >= 0 ? 'B' : 'S'}${(Math.abs(imb) * 100).toFixed(0)}%`, mid, H - 1);
}

// Audio: bootstrap the AudioContext on first user click (browser policy).
// We also re-arm both alerts so a price already past 90¢ at page-load can
// chime on the very next tick instead of needing to leave-and-return.
document.addEventListener('click', () => {
  if (!audioCtx){
    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    yesAlertArmed = true;
    noAlertArmed  = true;
    // Soft 200ms confirmation beep so you know audio works.
    const t = audioCtx.currentTime;
    const osc = audioCtx.createOscillator();
    const gain = audioCtx.createGain();
    osc.frequency.value = 660; osc.type = 'sine';
    gain.gain.setValueAtTime(0.0, t);
    gain.gain.linearRampToValueAtTime(0.18, t + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, t + 0.18);
    osc.connect(gain); gain.connect(audioCtx.destination);
    osc.start(t); osc.stop(t + 0.2);
    console.log('[audio] enabled — chime test played');
  }
}, { once: true });

function playChime(side){
  if (!audioCtx) return;  // user hasn't interacted yet — no sound until they click once
  const t = audioCtx.currentTime;
  // YES alert = bright ascending pair (E6 → E7-ish)
  // NO  alert = warm descending pair (C5 → G4) so they're audibly distinct
  const notes = side === 'yes'
    ? [[988, 0], [1318, 0.18]]
    : [[523, 0], [392, 0.20]];
  for (const [freq, offset] of notes){
    const osc = audioCtx.createOscillator();
    const gain = audioCtx.createGain();
    osc.frequency.value = freq;
    osc.type = 'sine';
    gain.gain.setValueAtTime(0.0, t + offset);
    gain.gain.linearRampToValueAtTime(0.35, t + offset + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, t + offset + 0.45);
    osc.connect(gain);
    gain.connect(audioCtx.destination);
    osc.start(t + offset);
    osc.stop(t + offset + 0.5);
  }
}

function checkAlert(){
  if (TICKS.length < 1) return;
  const last = TICKS[TICKS.length - 1];
  // YES side
  if (last.yes >= ALERT_THRESHOLD && yesAlertArmed){
    yesAlertArmed = false;
    playChime('yes');
  } else if (last.yes < ALERT_THRESHOLD - 2){
    yesAlertArmed = true;   // hysteresis re-arm
  }
  // NO side (independent — same threshold, different tone)
  if (last.no >= ALERT_THRESHOLD && noAlertArmed){
    noAlertArmed = false;
    playChime('no');
  } else if (last.no < ALERT_THRESHOLD - 2){
    noAlertArmed = true;
  }
}

function classifyFlow(dBid, dAsk){
  // |x| <  FLAT_THRESHOLD/s  →  treated as "flat"
  const FLAT = 50;
  const bU = dBid >  FLAT, bD = dBid < -FLAT, bF = !bU && !bD;
  const aU = dAsk >  FLAT, aD = dAsk < -FLAT, aF = !aU && !aD;
  // Returns [readable text, css color class: 'bull' | 'bear' | 'neutral']
  if (bU && aD) return ['Bullish — buyers piling in, sellers walking',  'bull'];
  if (bD && aU) return ['Bearish — buyers leaving, sellers stepping in','bear'];
  if (bU && aF) return ['Buying pressure building',                     'bull'];
  if (bF && aU) return ['Selling pressure building',                    'bear'];
  if (bU && aU) return ['Market liquefying — both sides interested',    'neutral'];
  if (bD && aD) return ['Liquidity drying up',                          'neutral'];
  if (bD && aF) return ['Buyers stepping away',                         'bear'];
  if (bF && aD) return ['Sellers stepping away',                        'bull'];
  return ['Quiet', 'neutral'];
}

function updateFlow(){
  // Snapshot current depth, drop samples older than window
  const now = Date.now();
  flowSamples.push({t: now, b: DEPTH.total_bid, a: DEPTH.total_ask});
  while (flowSamples.length && flowSamples[0].t < now - FLOW_WINDOW_MS){
    flowSamples.shift();
  }
  if (flowSamples.length < 2){
    document.getElementById('flow-ind').innerHTML = 'flow: —';
    return;
  }
  const first = flowSamples[0], last = flowSamples[flowSamples.length - 1];
  const dt = Math.max(1, (last.t - first.t) / 1000);   // seconds
  const dBid = (last.b - first.b) / dt;
  const dAsk = (last.a - first.a) / dt;
  const arrow = (v) => v >= 0 ? '▲' : '▼';
  const fmt = (v) => `${v >= 0 ? '+' : ''}${Math.round(v)}/s`;
  const [verdict, cls] = classifyFlow(dBid, dAsk);
  document.getElementById('flow-ind').innerHTML =
    `flow ${FLOW_WINDOW_MS/1000}s: ` +
    `<span class="flow-bid">${arrow(dBid)} bids ${fmt(dBid)}</span>  ` +
    `<span class="flow-ask">${arrow(dAsk)} asks ${fmt(dAsk)}</span>  ` +
    `<span class="flow-verdict ${cls}">· ${verdict}</span>`;
}

function drawPositions(){
  const list = document.getElementById('pos-list');
  if (!POSITIONS.length){
    list.innerHTML = '<div class="pos-empty">No open positions across portfolio</div>';
    return;
  }
  list.innerHTML = POSITIONS.map(p => {
    const ticker = p.ticker || p.market_ticker || '?';
    // Kalshi's current schema: position_fp is signed net contracts.
    //   > 0  → net YES, value = qty
    //   < 0  → net NO,  value = -qty
    //   = 0  → flat or perfectly hedged
    const netPos    = parseFloat(p.position_fp ?? p.position ?? 0);
    const traded    = parseFloat(p.total_traded_dollars ?? 0);
    const realized  = parseFloat(p.realized_pnl_dollars ?? 0);
    const exposure  = parseFloat(p.market_exposure_dollars ?? 0);
    const fees      = parseFloat(p.fees_paid_dollars ?? 0);

    // Side + qty for display
    let sideTxt, sideClass, qtyAbs;
    if      (netPos > 0){ sideTxt = 'YES';  sideClass = 'yes'; qtyAbs = netPos; }
    else if (netPos < 0){ sideTxt = 'NO';   sideClass = 'no';  qtyAbs = -netPos; }
    else                { sideTxt = 'FLAT'; sideClass = 'neutral'; qtyAbs = 0; }

    const isActive = ticker === TICKER;
    const realizedFmt = `${realized >= 0 ? '+' : ''}$${realized.toFixed(2)}`;
    const realizedClass = realized >= 0 ? 'pos' : 'neg';

    // Build the right-side action: SELL if directional, FLAT badge if hedged
    let action;
    if (netPos !== 0){
      const sellSide = netPos > 0 ? 'yes' : 'no';
      action = `<button class="pos-sell" onclick="sellPosition('${ticker}', '${sellSide}', ${Math.round(qtyAbs)})">SELL</button>`;
    } else {
      action = `<span class="pos-flat-badge">HEDGED</span>`;
    }

    return `<div class="pos-row">
      <span class="pos-ticker ${isActive ? 'active' : ''}" title="${ticker}">${ticker}</span>
      <span class="pos-side ${sideClass}">${sideTxt}${qtyAbs > 0 ? ' ' + Math.round(qtyAbs) : ''}</span>
      <span class="pos-qty">traded $${traded.toFixed(2)}</span>
      <span class="pos-cur">fees $${fees.toFixed(2)}</span>
      <span class="pos-pnl ${realizedClass}">P&L ${realizedFmt}</span>
      ${action}
    </div>`;
  }).join('');
}

async function loadPositions(){
  try {
    const r = await fetch('/positions');
    const data = await r.json();
    POSITIONS = data.positions || [];
    FILLS     = data.fills     || [];
    PRICES    = data.prices    || {};
    document.getElementById('pos-status').textContent =
      data.error ? `(error: ${data.error})` : `${POSITIONS.length} open · ${FILLS.length} fills`;
    drawPositions();
    drawFills();
  } catch (e){ console.error('positions fetch failed', e); }
}

function drawFills(){
  const list = document.getElementById('fill-list');
  if (!FILLS.length){
    list.innerHTML = '<div class="pos-empty">No recent fills</div>';
    return;
  }
  list.innerHTML = FILLS.map(f => {
    const ticker = f.ticker || f.market_ticker || '?';
    const side   = (f.side || '').toLowerCase();      // 'yes' or 'no'
    const action = (f.action || '').toLowerCase();    // 'buy' or 'sell'
    const qty    = parseFloat(f.count_fp || f.count || 0);
    // Fill price comes back as dollar string; convert to cents
    const priceD = side === 'yes'
      ? parseFloat(f.yes_price_dollars || 0)
      : parseFloat(f.no_price_dollars  || 0);
    const priceC = Math.round(priceD * 100);
    const ts = f.created_time || (f.ts ? new Date(f.ts * 1000).toISOString() : '');
    const timeShort = ts ? ts.slice(11, 19) : '';      // HH:MM:SS
    return `<div class="fill-row">
      <span class="fill-time">${timeShort}</span>
      <span class="fill-action ${action}">${action.toUpperCase()}</span>
      <span class="fill-side ${side}">${side.toUpperCase()}</span>
      <span class="fill-qty">${qty.toFixed(0)}</span>
      <span class="fill-price">@ ${priceC}¢</span>
      <span class="fill-ticker" title="${ticker}">${ticker}</span>
    </div>`;
  }).join('');
}

async function sellPosition(ticker, side, qty){
  try {
    const r = await fetch('/order', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ticker, side, count: qty, action: 'sell'}),
    });
    const data = await r.json();
    if (!data.ok) alert('Sell rejected: ' + data.error);
    setTimeout(loadPositions, 500);  // Refresh after Kalshi processes
  } catch (e){ alert('Network error: ' + e); }
}

function draw(){
  clampTimeView();
  drawChart();
  drawOrderbook();
  drawDepth();
  updateScrollbar();
}

// Trade buttons → POST /order. No confirm dialog: the server fills at the
// live ask whenever the request lands, not at any price shown in the UI.
async function trade(side){
  const qty = parseInt(document.getElementById('trade-qty').value) || 1;
  if (!TICKS.length) return;
  try {
    const r = await fetch('/order', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({side, count: qty}),
    });
    const data = await r.json();
    if (!data.ok) alert('Order rejected: ' + data.error);
    // Successful orders just go through silently — watch your terminal /
    // Kalshi portfolio for fills. Add an alert here if you want feedback.
  } catch (e){
    alert('Network error: ' + e);
  }
}

let sbDrag = false, sbX0 = 0, sbVTMin0 = 0, sbVTMax0 = 0;
document.getElementById('sb-thumb').addEventListener('mousedown', e => {
  sbDrag = true; sbX0 = e.clientX;
  sbVTMin0 = viewTMin; sbVTMax0 = viewTMax;
  autoscroll = false;
  document.getElementById('btn-autoscroll').classList.remove('active');
  e.target.classList.add('dragging'); e.preventDefault();
});
document.addEventListener('mousemove', e => {
  if (!sbDrag) return;
  const range = fullTimeRange();
  if (!range) return;
  const [absMin, absMax] = range;
  const tw = document.getElementById('sb-track').offsetWidth;
  const dx = e.clientX - sbX0;
  const shiftMs = (dx / tw) * (absMax - absMin);
  const span = sbVTMax0 - sbVTMin0;
  viewTMin = sbVTMin0 + shiftMs;
  viewTMax = viewTMin + span;
  draw();
});
document.addEventListener('mouseup', () => {
  if (sbDrag){ sbDrag = false; document.getElementById('sb-thumb').classList.remove('dragging'); }
});

let dragging = false, dx0 = 0, vtMin0 = 0, vtMax0 = 0;
let yDrag = false, yy0 = 0, yMin0 = 0, yMax0 = 0;

canvas.addEventListener('wheel', e => {
  e.preventDefault();
  const W = canvas.offsetWidth, H = canvas.offsetHeight;
  const pad = getPad();
  const inYEdge = e.offsetX < pad.left || e.offsetX > W - pad.right;

  if (inYEdge || e.shiftKey){
    const {yMin, yMax} = getY();
    const factor = e.deltaY > 0 ? 1.12 : 0.89;
    if (e.shiftKey && !inYEdge){
      const relY = e.offsetY;
      const inPlot = relY >= pad.top && relY <= H - pad.bottom;
      const anchor = inPlot ? yMax - ((relY - pad.top) / ph(H, pad)) * (yMax - yMin) : (yMin + yMax) / 2;
      const frac = (anchor - yMin) / (yMax - yMin);
      const newRange = (yMax - yMin) * factor;
      yState.min = anchor - frac * newRange;
      yState.max = anchor + (1 - frac) * newRange;
    } else {
      const c = (yMin + yMax) / 2;
      const r = (yMax - yMin) * factor;
      yState.min = c - r/2;
      yState.max = c + r/2;
    }
    yState.manual = true;
    draw(); return;
  }

  autoscroll = false;
  document.getElementById('btn-autoscroll').classList.remove('active');
  if (viewTMin == null || viewTMax == null) return;
  const frac = Math.max(0, Math.min(1, (e.offsetX - pad.left) / pw(W, pad)));
  const factor = e.deltaY > 0 ? 1.15 : 0.87;
  const anchorT = viewTMin + frac * (viewTMax - viewTMin);
  const newSpan = (viewTMax - viewTMin) * factor;
  viewTMin = anchorT - frac * newSpan;
  viewTMax = anchorT + (1 - frac) * newSpan;
  draw();
}, { passive: false });

canvas.addEventListener('mousedown', e => {
  const W = canvas.offsetWidth;
  const pad = getPad();
  const inY = e.offsetX > W - pad.right || e.offsetX < pad.left;
  if (inY){
    yDrag = true; yy0 = e.offsetY;
    const {yMin, yMax} = getY();
    yMin0 = yMin; yMax0 = yMax; yState.manual = true;
    canvas.style.cursor = 'ns-resize';
  } else {
    dragging = true; dx0 = e.offsetX;
    vtMin0 = viewTMin; vtMax0 = viewTMax;
    autoscroll = false;
    document.getElementById('btn-autoscroll').classList.remove('active');
    canvas.style.cursor = 'grabbing';
  }
  e.preventDefault();
});

canvas.addEventListener('mousemove', e => {
  const W = canvas.offsetWidth, H = canvas.offsetHeight;
  const pad = getPad();
  if (yDrag){
    const dy = e.offsetY - yy0;
    const range = yMax0 - yMin0;
    const shift = (dy / ph(H, pad)) * range;
    yState.min = yMin0 + shift;
    yState.max = yMax0 + shift;
    draw(); return;
  }
  if (dragging){
    const span = vtMax0 - vtMin0;
    const shiftT = -(e.offsetX - dx0) / pw(W, pad) * span;
    viewTMin = vtMin0 + shiftT;
    viewTMax = vtMax0 + shiftT;
    draw(); return;
  }
  const inY = e.offsetX > W - pad.right || e.offsetX < pad.left;
  canvas.style.cursor = inY ? 'ns-resize' : 'crosshair';

  if (viewTMin != null && viewTMax != null){
    const frac = Math.max(0, Math.min(1, (e.offsetX - pad.left) / pw(W, pad)));
    const t = viewTMin + frac * (viewTMax - viewTMin);
    if (t !== crosshairT){
      crosshairT = t;
      draw();
    }
    showTooltip(e);
  }
});

canvas.addEventListener('mouseup', () => { dragging = false; yDrag = false; canvas.style.cursor = 'crosshair'; });
canvas.addEventListener('mouseleave', () => {
  dragging = false; yDrag = false;
  canvas.style.cursor = 'crosshair';
  document.getElementById('tt').style.display = 'none';
  crosshairT = null;
  draw();
});

function showTooltip(e){
  if (crosshairT === null) return;
  const snap = snapAtTime(TICKS, crosshairT);
  if (!snap) return;
  let html = `
    <div class="tt-row"><span class="tt-label">Time</span><span>${fmtTime(crosshairT)}</span></div>
    <div class="tt-row"><span class="tt-label">YES</span><span class="tt-val g">${snap.yes}c</span></div>
    <div class="tt-row"><span class="tt-label">NO</span><span class="tt-val r">${snap.no}c</span></div>
  `;
  const tt = document.getElementById('tt');
  tt.innerHTML = html;
  tt.style.display = 'block';
  let x = e.clientX + 14, y = e.clientY + 14;
  if (x + 230 > window.innerWidth) x = e.clientX - 240;
  if (y + 200 > window.innerHeight) y = e.clientY - 210;
  tt.style.left = x + 'px'; tt.style.top = y + 'px';
}

function snapFullView(){
  const r = fullTimeRange();
  if (!r) return;
  [viewTMin, viewTMax] = r;
}

document.getElementById('btn-50').addEventListener('click', e => {
  show50 = !show50; e.currentTarget.classList.toggle('active', show50); draw();
});
document.getElementById('btn-y0100').addEventListener('click', e => {
  yPinned = !yPinned;
  e.currentTarget.classList.toggle('active', yPinned);
  yState = yPinned ? {min: 0, max: 100, manual: true} : {min: 0, max: 100, manual: false};
  draw();
});
document.getElementById('btn-autoscroll').addEventListener('click', e => {
  autoscroll = !autoscroll;
  e.currentTarget.classList.toggle('active', autoscroll);
  if (autoscroll){ snapFullView(); draw(); }
});
document.getElementById('btn-reset').addEventListener('click', () => {
  autoscroll = true; yPinned = false;
  document.getElementById('btn-autoscroll').classList.add('active');
  document.getElementById('btn-y0100').classList.remove('active');
  yState = {min: 0, max: 100, manual: false};
  snapFullView();
  draw();
});
document.getElementById('btc-range-sel').addEventListener('change', e => {
  const v = e.target.value;
  btcRangeMode = (v === 'auto') ? 'auto' : parseFloat(v);
  e.target.classList.toggle('locked', btcRangeMode !== 'auto');
  draw();
});
document.getElementById('btn-target-center').addEventListener('click', e => {
  targetCentered = !targetCentered;
  e.currentTarget.classList.toggle('active', targetCentered);
  draw();
});

window.addEventListener('resize', draw);

function setLive(on){
  const dot = document.getElementById('live-dot');
  const txt = document.getElementById('live-text');
  dot.classList.toggle('connected', !!on);
  txt.textContent = on ? 'Live' : 'Offline';
}

async function loadState(){
  const r = await fetch('/data');
  const s = await r.json();
  TICKS  = s.ticks || [];
  OB     = {bids: s.ob_bids || [], asks: s.ob_asks || []};
  DEPTH  = {
    buyer_lvls:  s.buyer_lvls  || 0,
    seller_lvls: s.seller_lvls || 0,
    total_bid:   s.total_bid   || 0,
    total_ask:   s.total_ask   || 0,
    imbalance:   s.imbalance   || 0,
  };
  TICKER = s.ticker; CLOSE = s.close_time;
  document.getElementById('hd-ticker').textContent = TICKER;
  document.getElementById('hd-close').textContent  = CLOSE;
  setLive(s.live);
  // Reflect trading state in the note next to the buy buttons
  const note = document.getElementById('trade-note');
  if (s.trading_enabled){
    note.textContent = '[LIVE — real orders]';
    note.style.color = '#ff8c42';
  } else {
    note.textContent = '[trading disabled]';
    note.style.color = '';
  }
  snapFullView();
  draw();
}

function connectSSE(){
  const es = new EventSource('/events');
  es.onerror = () => setLive(false);
  es.onmessage = ev => {
    try {
      const m = JSON.parse(ev.data);
      if (m.type === 'tick'){
        TICKS.push({ts: m.snap.ts, yes: m.snap.yes, no: m.snap.no, btc: m.snap.btc});
        if (m.snap.target != null) TARGET = m.snap.target;
        if (TICKS.length > 5000) TICKS.shift();
        if (m.snap.ob_bids) OB.bids = m.snap.ob_bids;
        if (m.snap.ob_asks) OB.asks = m.snap.ob_asks;
        DEPTH = {
          buyer_lvls:  m.snap.buyer_lvls  || 0,
          seller_lvls: m.snap.seller_lvls || 0,
          total_bid:   m.snap.total_bid   || 0,
          total_ask:   m.snap.total_ask   || 0,
          imbalance:   m.snap.imbalance   || 0,
        };
        if (autoscroll) snapFullView();
        setLive(m.live);
        draw();
        checkAlert();        // chime + arm/disarm 90¢ trigger
        updateFlow();        // refresh the rate-of-change indicator
        drawPositions();     // re-render P&L against the new last price
      } else if (m.type === 'status'){
        setLive(m.live);
      } else if (m.type === 'market'){
        TICKER = m.ticker; CLOSE = m.close_time;
        TICKS = []; OB = {bids: [], asks: []};
        TARGET = null;     // new market → next tick will set the new target
        document.getElementById('hd-ticker').textContent = TICKER;
        document.getElementById('hd-close').textContent  = CLOSE;
        snapFullView();
        draw();
      }
    } catch (e){ console.error(e); }
  };
}

loadState().then(connectSSE);
loadPositions();
setInterval(loadPositions, 3000);   // refresh open positions every 3s
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────
# FLASK ROUTES
# ─────────────────────────────────────────────────────────────
app         = Flask(__name__)
store       = DataStore()
broadcaster = SSEBroadcaster()
csv_logger  = CSVLogger(CSV_DIR)
api         = None
poller      = None
atexit.register(csv_logger.close)


@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/data")
def data():
    return jsonify(store.snapshot())


@app.route("/order", methods=["POST"])
def order():
    """Place a market buy order. Disabled unless TRADING_ENABLED is True."""
    if not TRADING_ENABLED:
        return jsonify({"ok": False,
                        "error": "Trading is disabled. Uncomment "
                                 "`TRADING_ENABLED = True` near the top of "
                                 "kalshi_scanner.py to enable."}), 403
    if api is None or poller is None or poller.btc_market is None:
        return jsonify({"ok": False, "error": "No active market."}), 409
    body   = freq.get_json(silent=True) or {}
    side   = body.get("side")
    count  = int(body.get("count", 1))
    action = body.get("action", "buy")
    # Optional: caller can pass a specific ticker (used by SELL on a position
    # for a different market than the one the chart is showing).
    override_ticker = body.get("ticker")
    if side not in ("yes", "no") or count < 1:
        return jsonify({"ok": False, "error": "side must be 'yes' or 'no'; count >= 1"}), 400
    if action not in ("buy", "sell"):
        return jsonify({"ok": False, "error": "action must be 'buy' or 'sell'"}), 400
    ticker = override_ticker or (poller.btc_market["ticker"]
                                 if (poller and poller.btc_market) else None)
    if not ticker:
        return jsonify({"ok": False, "error": "no ticker available"}), 409
    try:
        result = api.place_order(ticker, side, count, action=action)
        print(f"[order] {action.upper()} {side.upper()} x{count} {ticker} → {result}")
        order_obj = result.get("order") if isinstance(result, dict) else {}
        csv_logger.log_order({
            "timestamp_iso": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker, "action": action, "side": side, "count": count,
            "ok": True,
            "order_id": (order_obj or {}).get("order_id", ""),
            "status":   (order_obj or {}).get("status", ""),
            "error": "",
        })
        return jsonify({"ok": True, "result": result})
    except Exception as e:
        print(f"[order error] {e}")
        csv_logger.log_order({
            "timestamp_iso": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker, "action": action, "side": side, "count": count,
            "ok": False, "order_id": "", "status": "rejected", "error": str(e),
        })
        return jsonify({"ok": False, "error": str(e)}), 500


_positions_logged_once = False    # one-time schema dump so we can debug fields

@app.route("/positions")
def positions():
    """Return EVERY open position across the user's portfolio (not just the
    market the chart is showing), plus a {ticker → current YES/NO cents}
    price map so the frontend can compute live P&L for all of them."""
    global _positions_logged_once
    if api is None:
        return jsonify({"positions": [], "prices": {}, "ticker": None})
    active_ticker = poller.btc_market["ticker"] if (poller and poller.btc_market) else None
    try:
        data = api.get_positions()

        # One-time full dump so we can see Kalshi's actual response shape —
        # critical when the market_positions/event_positions/etc. keys vary.
        if not _positions_logged_once:
            print(f"[debug] /portfolio/positions full response: {json.dumps(data)[:2000]}")
            _positions_logged_once = True

        all_positions = (data.get("market_positions")
                         or data.get("event_positions")
                         or data.get("positions")
                         or [])

        # Keep any position with current activity. Kalshi schema:
        #   position_fp            = signed net contracts (YES > 0, NO < 0,
        #                            0 if perfectly hedged or fully closed)
        #   total_traded_dollars   = lifetime traded notional on this market
        #   resting_orders_count   = unfilled limit orders still open
        #   market_exposure_dollars= current at-risk dollars
        # A hedged position has position_fp == 0 but traded > 0 — we want
        # to show it because it still has realized P&L locked in.
        def has_activity(p):
            def f(k):
                try: return float(p.get(k, 0) or 0)
                except (TypeError, ValueError): return 0.0
            return (f("position_fp") != 0
                    or f("position")  != 0
                    or f("total_traded_dollars") != 0
                    or f("market_exposure_dollars") != 0
                    or int(p.get("resting_orders_count", 0) or 0) > 0)
        live = [p for p in all_positions if has_activity(p)]

        # Fetch current YES/NO mid prices for every unique ticker in the list
        prices = {}
        unique_tickers = {(p.get("ticker") or p.get("market_ticker")) for p in live}
        for t in filter(None, unique_tickers):
            try:
                m = api.get_market(t)
                def cents(v):
                    if v is None or v == "": return None
                    try: return float(v) * 100
                    except (TypeError, ValueError): return None
                bid  = cents(m.get("yes_bid_dollars"))
                ask  = cents(m.get("yes_ask_dollars"))
                last = cents(m.get("last_price_dollars"))
                if bid and ask and bid > 0 and ask > 0:
                    yes = (bid + ask) / 2
                elif ask and ask > 0: yes = ask
                elif bid and bid > 0: yes = bid
                elif last and last > 0: yes = last
                else: yes = 50.0
                prices[t] = {"yes": round(yes), "no": round(100 - yes)}
            except Exception as e:
                print(f"[positions] price fetch failed for {t}: {e}")
        # Also fetch recent fills — these persist after positions settle, so
        # the user always has a trade history to look at even when the
        # positions list is empty (which is most of the time on 15-min markets).
        fills = []
        try:
            fills_data = api.get_fills(limit=25)
            fills = fills_data.get("fills") or []
        except Exception as e:
            print(f"[fills error] {e}")

        return jsonify({"positions": live, "prices": prices,
                        "fills": fills, "ticker": active_ticker})
    except Exception as e:
        print(f"[positions error] {e}")
        return jsonify({"positions": [], "prices": {}, "fills": [],
                        "error": str(e), "ticker": active_ticker}), 200

@app.route("/events")
def events():
    def gen():
        q = broadcaster.subscribe()
        try:
            yield ": connected\n\n"
            while True:
                try:
                    payload = q.get(timeout=15)
                    yield f"data: {payload}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            broadcaster.unsubscribe(q)
    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not os.path.exists(KALSHI_PEM_PATH):
        print(f"ERROR: KALSHI_PEM_PATH does not exist: {KALSHI_PEM_PATH}")
        print("Edit KALSHI_PEM_PATH at the top of this file to point to your .pem.")
        sys.exit(1)

    api    = KalshiAPI(KALSHI_KEY_ID, KALSHI_PEM_PATH)
    poller = Poller(api, store, broadcaster, csv_logger=csv_logger)
    poller._refresh()
    poller.start(interval=0.25)   # 4 polls per second
    print(f"http://localhost:{PORT}")
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
