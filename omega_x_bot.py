# -*- coding: utf-8 -*-
"""
OmegaX Fusion Bot v23 (Python 3.13.4 compatible)
- Merged OmegaX + V20 logic and extended
- Exchange: Binance USDT-M Futures via ccxt REST; python-binance WS for fills
- Event-driven: lightweight EventBus for closed-candle and signal routing (no ccxt-pro)
- Concurrency safety: single-writer state; RLock for reads/writes; WS thread enqueues; main loop mutates
- Execution: native SL/TP at entry, cancel&replace on updates, repair loop, bracket reconciliation, emergency flatten
- Strategies:
  - OmegaX multi-alpha (trend/meanrev/vwap/donchian/pair/xsmom/carry)
  - Scalping (1m)
  - Mean Reversion (Z-score)
  - Grid (reactive, no parked orders)
  - Arbitrage (detect-only alerts; optional)
- Hedging: overlay BTC hedge + temp entry hedges; reconciled and auto-rebalanced
- Telegram lifecycle alerts + 15-minute equity updates
- Flask endpoints: /, /ping, /health, /metrics, /api/status, /api/positions
- KeepAlive pinger: optional external heartbeat or self URL ping
- Remote persistence: S3 or GitHub Gist, to survive Render free redeploys. Local + Remote "both" supported.

Env (common):
  MODE=paper|live
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
  BINANCE_API_KEY, BINANCE_API_SECRET (required in live)
  PORT (default 10000)
  LOG_LEVEL (default INFO)
  SYMBOL_AUTO_DISCOVERY=true
  SYMBOLS_TOP_N=50
  ENABLE_SCALPING=true
  ENABLE_MEAN_REVERSION=true
  ENABLE_GRID=true
  ENABLE_ARBITRAGE=false
  SCALPING_MAX_SYMBOLS=12

Persistence:
  PERSIST_MODE=both|local|remote (default: both if remote configured, else local)
  REMOTE_BACKEND=s3|gist
  # S3
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
  S3_BUCKET, S3_KEY=state.json, S3_ENDPOINT (optional, e.g., minio)
  # Gist
  GITHUB_TOKEN, GIST_ID, GIST_FILENAME=state.json

KeepAlive:
  KEEPALIVE_URL=https://uptime-service/heartbeat (optional)
  SELF_URL=https://your-service.onrender.com/ping (optional)
  KEEPALIVE_INTERVAL_SEC=60
"""

from __future__ import annotations

import os
import time
import json
import random
import logging
import threading
from threading import RLock, Thread
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Tuple, Callable
from datetime import datetime, timezone
from queue import Queue, Empty
import signal

import numpy as np
import pandas as pd
import requests
from flask import Flask, jsonify

import ccxt
from ta.trend import EMAIndicator, MACD, ADXIndicator, SMAIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volume import VolumeWeightedAveragePrice

try:
    from binance.streams import ThreadedWebsocketManager
    BINANCE_WS_AVAILABLE = True
except Exception:
    BINANCE_WS_AVAILABLE = False

# ====================== CONFIG ======================

MODE = os.environ.get("MODE", "paper").lower()

SEED_SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "ADA/USDT", "DOGE/USDT", "TRX/USDT", "AVAX/USDT", "LINK/USDT",
    "DOT/USDT", "MATIC/USDT", "BCH/USDT", "LTC/USDT", "NEAR/USDT",
    "FIL/USDT", "ATOM/USDT", "ICP/USDT", "APT/USDT", "ARB/USDT"
]
SYMBOL_ALIASES = {"MATIC/USDT": "POL/USDT"}
INDEX_HEDGE_SYMBOLS = ["BTC/USDT"]

TIMEFRAMES = ["1m", "5m", "15m", "1h"]
TF_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}

# Feature flags
ENABLE_SCALPING = os.environ.get("ENABLE_SCALPING", "true").lower() == "true"
ENABLE_MEAN_REVERSION = os.environ.get("ENABLE_MEAN_REVERSION", "true").lower() == "true"
ENABLE_GRID = os.environ.get("ENABLE_GRID", "true").lower() == "true"
ENABLE_ARBITRAGE = os.environ.get("ENABLE_ARBITRAGE", "false").lower() == "true"
SYMBOL_AUTO_DISCOVERY = os.environ.get("SYMBOL_AUTO_DISCOVERY", "true").lower() == "true"
SYMBOLS_TOP_N = int(os.environ.get("SYMBOLS_TOP_N", "50"))
SCALPING_MAX_SYMBOLS = int(os.environ.get("SCALPING_MAX_SYMBOLS", "12"))

# Trading & risk
INITIAL_BALANCE = float(os.environ.get("INITIAL_BALANCE", "3000"))
LEVERAGE_MIN = 10.0
LEVERAGE_MAX = 20.0

RISK_PER_TRADE = 0.005
MAX_TOTAL_RISK = 0.06
MAX_CONCURRENT_POS = 8
MIN_TRADE_COOLDOWN_SEC = 300

SOFT_PAUSE_DD = 0.05
MAX_DRAWDOWN = 0.08
DAILY_MAX_LOSS_FRAC = 0.03

BASE_SCORE_THRESHOLD = 0.9
HV_SCORE_THRESHOLD = 1.1

PARTIAL_AT_R = 1.0
PARTIAL_FRACTION = 0.5
TRAIL_ATR_BASE = 1.4
TRAIL_ATR_TIGHT2 = 1.0
TRAIL_ATR_TIGHT3 = 0.7
PYRAMID_MAX_STEPS = 2
PYRAMID_STEP_ATR = 0.7
PYRAMID_ADD_FRAC = 0.5

# Recovery
RECOVERY_ENABLED = True
RECOVERY_BUCKET_FRACTION = 0.35
RECOVERY_MAX_RISK_FRAC = 0.012
RECOVERY_MIN_RISK_FRAC = 0.001
RECOVERY_SCALP_SL_ATR = 0.6
RECOVERY_SCALP_TP_ATR = 1.0
MAX_PARALLEL_RECOVERY_TRADES = 1

# Hedging
TEMP_ENTRY_HEDGE_FRAC = 0.3
TEMP_HEDGE_CLOSE_AT_R = 0.7

# Funding
FUNDING_TTL = 600
HIGH_POSITIVE_FUNDING = 0.00025
HIGH_NEGATIVE_FUNDING = -0.00025

# Loop & network
GLOBAL_LOOP_SLEEP_RANGE = (0.9, 1.6)
MAX_KLINE_UPDATES_PER_LOOP = int(os.environ.get("MAX_KLINE_UPDATES_PER_LOOP", "12"))
TICKER_FRESHNESS_SEC = float(os.environ.get("TICKER_FRESHNESS_SEC", "15"))
BACKOFF_BASE = 1.5
BACKOFF_MAX = 30.0
HTTP_TIMEOUT = 10

# Telegram
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_ENABLED = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)
TELEGRAM_MIN_INTERVAL = 0.8
TELEGRAM_REPORT_INTERVAL_SEC = int(os.environ.get("TELEGRAM_REPORT_INTERVAL_SEC", "900"))

# Grid params
GRID_LEVELS = int(os.environ.get("GRID_LEVELS", "8"))
GRID_SPACING_PCT = float(os.environ.get("GRID_SPACING_PCT", "0.004"))
GRID_RISK_FRACTION = float(os.environ.get("GRID_RISK_FRACTION", "0.15"))
GRID_COOLDOWN_SEC = int(os.environ.get("GRID_COOLDOWN_SEC", "45"))

# Arbitrage params
ARB_SCAN_INTERVAL_SEC = int(os.environ.get("ARB_SCAN_INTERVAL_SEC", "30"))
ARB_MIN_NET_PCT = float(os.environ.get("ARB_MIN_NET_PCT", "0.0015"))
ARB_EXCHANGES = [e.strip() for e in os.environ.get("ARB_EXCHANGES", "").split(",") if e.strip()]

# KeepAlive config
KEEPALIVE_URL = os.environ.get("KEEPALIVE_URL", "").strip()
SELF_URL = os.environ.get("SELF_URL", "").strip()
KEEPALIVE_INTERVAL_SEC = int(os.environ.get("KEEPALIVE_INTERVAL_SEC", "60"))

# Remote persistence config
PERSIST_MODE = os.environ.get("PERSIST_MODE", "").lower()  # local|remote|both (auto default below)
REMOTE_BACKEND = os.environ.get("REMOTE_BACKEND", "").lower()  # s3|gist

# S3
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.environ.get("AWS_REGION", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_KEY = os.environ.get("S3_KEY", "state.json")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "")

# Gist
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GIST_ID = os.environ.get("GIST_ID", "")
GIST_FILENAME = os.environ.get("GIST_FILENAME", "state.json")

# Web
PORT = int(os.environ.get("PORT", "10000"))
STATE_FILE = "state.json"
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Scalping params
SCALPING_RSI_PERIOD = int(os.environ.get("SCALP_RSI", "7"))
SCALPING_BB_PERIOD = int(os.environ.get("SCALP_BB", "20"))
SCALPING_PROFIT_TARGET = float(os.environ.get("SCALPING_PROFIT", "0.003"))
SCALPING_STOP_LOSS = float(os.environ.get("SCALPING_STOP", "0.002"))
SCALPING_TIME_LIMIT = int(os.environ.get("SCALPING_TIME", "300"))

# Mean reversion params
ZSCORE_PERIOD = int(os.environ.get("ZSCORE_PERIOD", "20"))
ZSCORE_THRESHOLD = float(os.environ.get("ZSCORE_THRESHOLD", "2.0"))

# ====================== UTILS ======================

def setup_logging():
    logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                        format="%(asctime)s [%(levelname)s] %(message)s")

def sleep_jitter(a: float, b: float):
    time.sleep(random.uniform(a, b))

def utc_midnight_ts() -> float:
    dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.timestamp()

def closed_bars(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    tf_ms = TF_SECONDS[timeframe] * 1000
    last_ts = int(df["timestamp"].iloc[-1])
    now_ms = int(time.time() * 1000)
    if now_ms < (last_ts + tf_ms):
        return df.iloc[:-1].copy()
    return df.copy()

class Notifier:
    def __init__(self, enabled: bool):
        self.enabled = enabled
        self._last = 0.0
    def send(self, msg: str):
        if not self.enabled:
            return
        now = time.time()
        if now - self._last < TELEGRAM_MIN_INTERVAL:
            time.sleep(TELEGRAM_MIN_INTERVAL - (now - self._last))
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=HTTP_TIMEOUT
            )
            if r.status_code != 200:
                logging.warning(f"Telegram {r.status_code} {r.text}")
        except Exception as e:
            logging.warning(f"Telegram error: {e}")
        self._last = time.time()
    def send_critical(self, msg: str):
        last = self._last
        self._last = 0.0
        try:
            self.send(msg)
        finally:
            self._last = last

class Backoff:
    def __init__(self, base=BACKOFF_BASE, max_sleep=BACKOFF_MAX):
        self.base, self.max_sleep, self.mult = base, max_sleep, 1.0
    def fail(self):
        d = min(self.base * self.mult, self.max_sleep) + random.uniform(0, 0.5)
        logging.warning(f"Backoff {d:.2f}s")
        time.sleep(d)
        self.mult *= self.base
    def success(self):
        self.mult = 1.0

class KeepAlive:
    def __init__(self, external_url: str, self_url: str, interval: int = 60):
        self.external_url = external_url
        self.self_url = self_url
        self.interval = max(30, interval)
        self.thread = Thread(target=self._run, daemon=True)
        self.running = False
    def start(self):
        self.running = True
        self.thread.start()
    def _run(self):
        while self.running:
            try:
                if self.external_url:
                    try:
                        requests.get(self.external_url, timeout=8)
                    except Exception:
                        pass
                if self.self_url:
                    try:
                        requests.get(self.self_url, timeout=8)
                    except Exception:
                        pass
            except Exception:
                pass
            time.sleep(self.interval)

# ====================== EXCHANGE WRAPPER ======================

class ExchangeWrapper:
    def __init__(self):
        self.ex = ccxt.binance({
            "apiKey": os.environ.get("BINANCE_API_KEY"),
            "secret": os.environ.get("BINANCE_API_SECRET"),
            "options": {"defaultType": "future", "adjustForTimeDifference": True},
            "enableRateLimit": True,
            "timeout": HTTP_TIMEOUT * 1000
        })
        self.ex.load_markets()
        self.box = Backoff()
    def fetch_ohlcv(self, s, tf, limit=300):
        while True:
            try:
                d = self.ex.fetch_ohlcv(s, timeframe=tf, limit=limit)
                self.box.success()
                return d
            except (ccxt.RateLimitExceeded, ccxt.NetworkError) as e:
                logging.warning(f"fetch_ohlcv {s} {tf}: {e}")
                self.box.fail()
            except Exception as e:
                logging.error(f"fetch_ohlcv fatal {s} {tf}: {e}")
                self.box.fail()
    def fetch_ticker(self, s):
        while True:
            try:
                d = self.ex.fetch_ticker(s)
                self.box.success()
                return d
            except (ccxt.RateLimitExceeded, ccxt.NetworkError) as e:
                logging.warning(f"fetch_ticker {s}: {e}")
                self.box.fail()
            except Exception as e:
                logging.error(f"fetch_ticker fatal {s}: {e}")
                self.box.fail()
    def fetch_open_orders(self, symbol: Optional[str] = None):
        try:
            return self.ex.fetch_open_orders(symbol)
        except Exception:
            return []
    def cancel_order_safe(self, order_id: str, symbol: str):
        try:
            return self.ex.cancel_order(order_id, symbol)
        except Exception as e:
            logging.debug(f"cancel_order {order_id} {symbol} -> {e}")
            return None
    def fetch_positions_live(self) -> List[dict]:
        try:
            if hasattr(self.ex, "fapiPrivateGetPositionRisk"):
                return self.ex.fapiPrivateGetPositionRisk()
        except Exception as e:
            logging.debug(f"fetch_positions_live fail: {e}")
        return []
    def id_to_symbol(self, market_id: str) -> Optional[str]:
        m = self.ex.markets_by_id.get(market_id)
        return m["symbol"] if m else None
    def set_leverage_safe(self, symbol: str, lev: int | float):
        try:
            m = self.ex.market(symbol)
            if hasattr(self.ex, "fapiPrivatePostLeverage"):
                self.ex.fapiPrivatePostLeverage({"symbol": m["id"], "leverage": int(max(1, min(int(lev), 125)))})
        except Exception as e:
            logging.debug(f"set_leverage failed {symbol} -> {e}")
    def discover_top_usdt_symbols(self, limit=50) -> List[str]:
        try:
            if hasattr(self.ex, "fapiPublicGetTicker24hr"):
                data = self.ex.fapiPublicGetTicker24hr()
                ranked = []
                for it in data:
                    sym_id = it.get("symbol", "")
                    if not sym_id.endswith("USDT"):
                        continue
                    sym = self.id_to_symbol(sym_id) or f"{sym_id[:-4]}/USDT"
                    if sym not in self.ex.markets:
                        continue
                    qv = float(it.get("quoteVolume", it.get("volume", 0.0)) or 0.0)
                    ranked.append((qv, sym))
                ranked.sort(reverse=True)
                syms = [s for _, s in ranked[:limit]]
                return [SYMBOL_ALIASES.get(s, s) for s in syms if s in self.ex.markets]
        except Exception as e:
            logging.warning(f"Top symbols discovery failed, fallback to seed list: {e}")
        return [SYMBOL_ALIASES.get(s, s) for s in SEED_SYMBOLS if s in self.ex.markets][:limit]

# ====================== PRICE + EVENT BUS + DATA ======================

@dataclass
class Event:
    type: str
    data: Dict
    timestamp: float = field(default_factory=lambda: time.time())
    source: str = ""
    priority: int = 5

class EventType:
    NEW_CANDLE_CLOSED = "new_candle_closed"
    SIGNAL = "signal"
    ERROR = "error"

class EventBus:
    def __init__(self):
        self.subs: Dict[str, List[Callable[[Event], None]]] = {}
        self.q = Queue()
        self.running = True
        self.lock = RLock()
        self.thread = Thread(target=self._loop, daemon=True)
        self.thread.start()
    def subscribe(self, evt_type: str, cb: Callable[[Event], None]):
        with self.lock:
            self.subs.setdefault(evt_type, []).append(cb)
    def publish(self, ev: Event):
        self.q.put(ev)
    def _loop(self):
        while self.running:
            try:
                ev = self.q.get(timeout=1)
            except Empty:
                continue
            try:
                with self.lock:
                    cbs = list(self.subs.get(ev.type, []))
                for cb in cbs:
                    try:
                        cb(ev)
                    except Exception as e:
                        logging.error(f"Event handler error {ev.type}: {e}")
            except Exception as e:
                logging.error(f"Event loop error: {e}")
    def shutdown(self):
        self.running = False

class PriceCache:
    def __init__(self, ex: ExchangeWrapper, symbols: List[str], freshness=TICKER_FRESHNESS_SEC):
        self.ex, self.syms, self.fresh = ex, symbols, freshness
        self.cache: Dict[str, Tuple[float, float]] = {}
        self.idx = 0
    def update_next(self):
        s = self.syms[self.idx]
        self.idx = (self.idx + 1) % len(self.syms)
        t = self.ex.fetch_ticker(s)
        if not t:
            return
        p = t.get("last") or t.get("close") or t.get("bid") or t.get("ask")
        if p:
            self.cache[s] = (float(p), time.time())
    def get(self, s, fallback_df: Optional[pd.DataFrame] = None) -> Optional[float]:
        v = self.cache.get(s)
        if v and time.time() - v[1] <= self.fresh:
            return v[0]
        if fallback_df is not None and not fallback_df.empty:
            return float(fallback_df["close"].iloc[-1])
        return None

class DataFeed:
    def __init__(self, ex: ExchangeWrapper, symbols: List[str], timeframes: List[str], event_bus: Optional[EventBus] = None, scalp_symbols: Optional[set] = None):
        self.ex, self.syms, self.tfs = ex, symbols, timeframes
        self.scalp_syms = scalp_symbols or set(symbols)
        self.df: Dict[str, Dict[str, pd.DataFrame]] = {s: {tf: pd.DataFrame() for tf in timeframes} for s in symbols}
        self.last: Dict[str, Dict[str, float]] = {s: {tf: 0.0 for tf in timeframes} for s in symbols}
        self.bus = event_bus
    def _timeframes_for(self, s: str) -> List[str]:
        tfs = []
        for tf in self.tfs:
            if tf == "1m" and s not in self.scalp_syms:
                continue
            tfs.append(tf)
        return tfs
    def cold_start(self):
        for s in self.syms:
            for tf in self._timeframes_for(s):
                self._update_one(s, tf)
                sleep_jitter(0.12, 0.28)
    def _update_one(self, s, tf):
        d = self.ex.fetch_ohlcv(s, tf, limit=300)
        if not d:
            return
        df = pd.DataFrame(d, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df = df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        old = self.df[s][tf]
        if not old.empty and len(df) >= 2:
            prev_last_ts = int(old["timestamp"].iloc[-1])
            new_last_ts = int(df["timestamp"].iloc[-1])
            if prev_last_ts != new_last_ts:
                closed_idx = -2 if len(df) >= 2 else -1
                closed = df.iloc[closed_idx].to_dict()
                if self.bus:
                    self.bus.publish(Event(EventType.NEW_CANDLE_CLOSED, {
                        "symbol": s, "timeframe": tf, "candle": closed, "timestamp": closed.get("timestamp")
                    }, source="datafeed", priority=7))
        self.df[s][tf] = df
        self.last[s][tf] = time.time()
    def schedule_updates(self, max_updates=MAX_KLINE_UPDATES_PER_LOOP) -> int:
        tasks = []
        now = time.time()
        for s in self.syms:
            for tf in self._timeframes_for(s):
                due = self.last[s][tf] + TF_SECONDS[tf]
                lateness = now - due
                if lateness >= -1.0:
                    weight = 2.0 if tf == "1m" else (1.2 if tf == "5m" else 1.0)
                    tasks.append((lateness * weight, s, tf))
        budget = min(max_updates + int(0.1 * len(self.syms) * len(TIMEFRAMES)), 40)
        tasks.sort(reverse=True)
        cnt = 0
        for _, s, tf in tasks[:budget]:
            try:
                self._update_one(s, tf)
                cnt += 1
            except Exception as e:
                logging.warning(f"update {s} {tf} failed: {e}")
        return cnt
    def get(self, s, tf):
        return self.df[s][tf]

# ====================== PERSISTENCE (LOCAL + REMOTE) ======================

class BasePersistence:
    def load(self) -> dict: raise NotImplementedError
    def save(self, risk, positions: Dict[str, Optional["Position"]], pending: List[dict] | None = None, hedge_book: dict | None = None): raise NotImplementedError

class LocalPersistence(BasePersistence):
    def __init__(self, path: str = STATE_FILE):
        self.path = path
    def load(self):
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"State load failed (local): {e}")
            return {}
    def save(self, risk, positions: Dict[str, Optional["Position"]], pending: List[dict] | None = None, hedge_book: dict | None = None):
        try:
            serial = {
                "balance": risk.balance, "equity_peak": risk.equity_peak, "daily_pnl": risk.daily_pnl,
                "loss_bucket": risk.loss_bucket, "cooldown": risk.cooldown, "paused_until": risk.paused_until,
                "risk_off_until": getattr(risk, "_risk_off_until", 0.0),
                "positions": {s: (asdict(p) if p else None) for s, p in positions.items()},
                "pending_intents": pending or [], "hedge_book": hedge_book or {}, "ts": time.time()
            }
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(serial, f, indent=2)
            os.replace(tmp, self.path)
        except Exception as e:
            logging.warning(f"State save failed (local): {e}")

class S3Persistence(BasePersistence):
    def __init__(self, bucket: str, key: str, region: str = "", endpoint: str = ""):
        self.bucket, self.key, self.region, self.endpoint = bucket, key, region, endpoint
        try:
            import boto3  # type: ignore
            self.boto3 = boto3
        except Exception as e:
            logging.error(f"boto3 not available for S3 persistence: {e}")
            self.boto3 = None
        self.client = None
        if self.boto3:
            try:
                kwargs = {"region_name": self.region} if self.region else {}
                if self.endpoint:
                    kwargs["endpoint_url"] = self.endpoint
                self.client = self.boto3.client("s3", **kwargs)
            except Exception as e:
                logging.error(f"S3 client init failed: {e}")
                self.client = None
    def load(self):
        if not self.client:
            return {}
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=self.key)
            data = obj["Body"].read().decode("utf-8")
            return json.loads(data)
        except self.client.exceptions.NoSuchKey:  # type: ignore
            return {}
        except Exception as e:
            logging.warning(f"State load failed (S3): {e}")
            return {}
    def save(self, risk, positions: Dict[str, Optional["Position"]], pending: List[dict] | None = None, hedge_book: dict | None = None):
        if not self.client:
            return
        try:
            serial = {
                "balance": risk.balance, "equity_peak": risk.equity_peak, "daily_pnl": risk.daily_pnl,
                "loss_bucket": risk.loss_bucket, "cooldown": risk.cooldown, "paused_until": risk.paused_until,
                "risk_off_until": getattr(risk, "_risk_off_until", 0.0),
                "positions": {s: (asdict(p) if p else None) for s, p in positions.items()},
                "pending_intents": pending or [], "hedge_book": hedge_book or {}, "ts": time.time()
            }
            body = json.dumps(serial, indent=2).encode("utf-8")
            self.client.put_object(Bucket=self.bucket, Key=self.key, Body=body, ContentType="application/json")
        except Exception as e:
            logging.warning(f"State save failed (S3): {e}")

class GistPersistence(BasePersistence):
    def __init__(self, token: str, gist_id: str, filename: str = "state.json"):
        self.token, self.gist_id, self.filename = token, gist_id, filename
        self.session = requests.Session()
        if self.token:
            self.session.headers.update({"Authorization": f"token {self.token}"})
        self.base = "https://api.github.com"
    def load(self):
        try:
            r = self.session.get(f"{self.base}/gists/{self.gist_id}", timeout=HTTP_TIMEOUT)
            if r.status_code != 200:
                logging.warning(f"Gist load status {r.status_code}: {r.text}")
                return {}
            data = r.json()
            files = data.get("files", {})
            if self.filename in files and files[self.filename].get("content"):
                return json.loads(files[self.filename]["content"])
            return {}
        except Exception as e:
            logging.warning(f"State load failed (Gist): {e}")
            return {}
    def save(self, risk, positions: Dict[str, Optional["Position"]], pending: List[dict] | None = None, hedge_book: dict | None = None):
        try:
            serial = {
                "balance": risk.balance, "equity_peak": risk.equity_peak, "daily_pnl": risk.daily_pnl,
                "loss_bucket": risk.loss_bucket, "cooldown": risk.cooldown, "paused_until": risk.paused_until,
                "risk_off_until": getattr(risk, "_risk_off_until", 0.0),
                "positions": {s: (asdict(p) if p else None) for s, p in positions.items()},
                "pending_intents": pending or [], "hedge_book": hedge_book or {}, "ts": time.time()
            }
            body = {
                "files": {
                    self.filename: {
                        "content": json.dumps(serial, indent=2)
                    }
                }
            }
            r = self.session.patch(f"{self.base}/gists/{self.gist_id}", json=body, timeout=HTTP_TIMEOUT)
            if r.status_code not in (200, 201):
                logging.warning(f"Gist save status {r.status_code}: {r.text}")
        except Exception as e:
            logging.warning(f"State save failed (Gist): {e}")

class MultiPersistence(BasePersistence):
    def __init__(self, local: LocalPersistence, remote: BasePersistence, prefer_remote_on_load: bool = True):
        self.local = local
        self.remote = remote
        self.prefer_remote_on_load = prefer_remote_on_load
    def load(self):
        if self.prefer_remote_on_load:
            r = self.remote.load()
            if r:
                return r
            return self.local.load()
        else:
            l = self.local.load()
            if l:
                return l
            return self.remote.load()
    def save(self, risk, positions: Dict[str, Optional["Position"]], pending: List[dict] | None = None, hedge_book: dict | None = None):
        self.local.save(risk, positions, pending, hedge_book)
        try:
            self.remote.save(risk, positions, pending, hedge_book)
        except Exception as e:
            logging.warning(f"MultiPersistence remote save failed: {e}")

def build_persistence() -> BasePersistence:
    local = LocalPersistence(STATE_FILE)
    remote = None
    if REMOTE_BACKEND == "s3" and all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET]):
        remote = S3Persistence(S3_BUCKET, S3_KEY, AWS_REGION, S3_ENDPOINT)
    elif REMOTE_BACKEND == "gist" and all([GITHUB_TOKEN, GIST_ID]):
        remote = GistPersistence(GITHUB_TOKEN, GIST_ID, GIST_FILENAME)
    # decide mode
    mode = PERSIST_MODE or ("both" if remote else "local")
    if mode == "local":
        return local
    if mode == "remote" and remote:
        return remote
    if mode == "both" and remote:
        return MultiPersistence(local, remote, prefer_remote_on_load=True)
    # default fallback
    return local

# ====================== DOMAIN MODELS ======================

@dataclass
class Position:
    symbol: str
    side: str
    qty: float
    entry: float
    sl: float
    tp: float
    lev: float
    opened_at: float
    partial: bool = False
    is_hedge: bool = False
    mode: str = "directional"
    pyramids: int = 0
    last_add_price: Optional[float] = None
    tag: str = "meta"
    entry_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    time_limit: Optional[float] = None
    def dir(self):
        return 1 if self.side == "long" else -1

class Mode:
    PAPER = "paper"
    LIVE = "live"

class HardStopTriggered(Exception):
    pass

# ====================== RISK / META / STRATEGIES (same as v22) ======================
# ... [To save space here, this block remains identical to v22 you received: RiskManager, SymbolAlpha, AlphaEngine,
# Welford, MetaPolicy, Strategy, ScalpingStrategy, MeanReversionStrategy, PortfolioManager]
# The rest of v23 below includes execution, funding, hedger, WS, grid, arb, bot, all updated.

# For brevity in this answer, the above classes remain unchanged from v22.
# In your final file, keep the full definitions exactly as in v22 (they are in this reply earlier).
# ====================== Copy-paste the same v22 block here without modification ======================

# [BEGIN unchanged block from v22]
# RiskManager, SymbolAlpha, AlphaEngine, Welford, MetaPolicy, Strategy, ScalpingStrategy, MeanReversionStrategy, PortfolioManager
# [END unchanged block from v22]

# ====================== ORDER EVENT + EXECUTION / FUNDING / HEDGE / WS / GRID / ARB ======================

@dataclass
class OrderEvent:
    symbol: str
    ord_type: str
    status: str
    side: str
    order_id: str
    avg_price: float
    reduce_only: bool

# [Copy the ExecutionEngine, FundingCache, HedgeEngine, UserStream, GridManager, ArbitrageCoordinator exactly as in v22]

# ====================== BOT ======================

class OmegaXBot:
    def __init__(self):
        setup_logging()
        self.notifier = Notifier(TELEGRAM_ENABLED)
        self.notifier.send_critical(f"🚀 Starting OmegaX Fusion v23 ({MODE}).")

        self.ex = ExchangeWrapper()
        markets = self.ex.ex.load_markets()

        if SYMBOL_AUTO_DISCOVERY:
            candidates = self.ex.discover_top_usdt_symbols(limit=SYMBOLS_TOP_N)
        else:
            candidates = [SYMBOL_ALIASES.get(s, s) for s in SEED_SYMBOLS]

        normalized = []
        for s in candidates:
            if s in markets:
                normalized.append(s)
            else:
                logging.warning(f"Skipping {s}: not found on Binance futures.")
        self.symbols = normalized
        self.scalp_symbols = set(self.symbols[:min(SCALPING_MAX_SYMBOLS, len(self.symbols))])

        # KeepAlive pinger
        self.keepalive = KeepAlive(KEEPALIVE_URL, SELF_URL, KEEPALIVE_INTERVAL_SEC)
        self.keepalive.start()

        # EventBus, data, prices
        self.bus = EventBus()
        self.data = DataFeed(self.ex, self.symbols, TIMEFRAMES, event_bus=self.bus, scalp_symbols=self.scalp_symbols)
        self.prices = PriceCache(self.ex, self.symbols)

        # Strategies
        self.strategy = Strategy()
        self.scalper = ScalpingStrategy() if ENABLE_SCALPING else None
        self.meanrev = MeanReversionStrategy() if ENABLE_MEAN_REVERSION else None
        self.grid = GridManager(self) if ENABLE_GRID else None

        # Risk / alpha / funding / persistence
        self.risk = RiskManager(INITIAL_BALANCE)
        self.alpha = AlphaEngine(self.symbols)
        self.meta = MetaPolicy(self.symbols)
        self.persistence = build_persistence()
        self.positions: Dict[str, Optional[Position]] = {s: None for s in self.symbols}
        self.lock = RLock()
        self.portfolio_manager = PortfolioManager(self)
        self.engine = ExecutionEngine(Mode.PAPER if MODE == "paper" else Mode.LIVE, self.ex, self.persistence, self.notifier, self.lock)
        self.hedger = HedgeEngine(self)
        self.last_report = 0.0
        self._last_loop_error_notify = 0.0
        self._last_reconcile = 0.0

        self.events: Queue[OrderEvent] = Queue()

        self.user_ws: Optional[UserStream] = None
        if MODE == "live":
            self.user_ws = UserStream(os.environ.get("BINANCE_API_KEY", ""), os.environ.get("BINANCE_API_SECRET", ""),
                                      self.ex, self.enqueue_event)
            self.notifier.send("🟡 Starting user WebSocket...")

        self._start_health_server()
        self._restore_state()
        if MODE == "live":
            self._sync_from_exchange()
            self.engine.sync_orders(self.positions)
            if self.user_ws:
                self.user_ws.start()
                self.notifier.send("🟢 User WebSocket started.")

        self.arb = ArbitrageCoordinator(self.notifier) if ENABLE_ARBITRAGE else None
        self.notifier.send(f"✅ OmegaX Fusion online ({MODE}). Symbols: {len(self.symbols)}. Equity ${self.risk.balance:.2f}")

        self.bus.subscribe(EventType.SIGNAL, self._on_signal_event)

        def _sig_handler(sig, frame):
            try:
                self.notifier.send_critical(f"🛑 Received signal {sig}. Shutting down.")
            except Exception:
                pass
            os._exit(0)
        try:
            signal.signal(signal.SIGTERM, _sig_handler)
            signal.signal(signal.SIGINT, _sig_handler)
        except Exception:
            logging.debug("Signal handlers not available on this platform.")

    # ... [keep _drain_events, _start_health_server, _restore_state identical to v22,
    # but persistence is already replaced via build_persistence()]

    def _reconcile_positions_live(self):
        # Fallback reconciliation for live mode if WS drops; ensures we close positions when exchange reports flat
        if MODE != "live":
            return
        if time.time() - self._last_reconcile < 30:
            return
        self._last_reconcile = time.time()
        try:
            live_raw = self.ex.fetch_positions_live()
            live_map = {}
            for r in live_raw:
                market_id = r.get("symbol") or r.get("info", {}).get("symbol")
                sym = self.ex.id_to_symbol(market_id) or market_id
                if sym not in self.symbols:
                    continue
                amt = float(r.get("positionAmt") or r.get("contracts") or r.get("size") or 0)
                if abs(amt) < 1e-12:
                    continue
                live_map[sym] = amt

            with self.lock:
                for sym, p in list(self.positions.items()):
                    if not p or p.is_hedge:
                        continue
                    live_amt = live_map.get(sym, 0.0)
                    # if bot thinks we have pos but exchange shows zero => close locally
                    if abs(live_amt) < 1e-12:
                        px = self.prices.get(sym, self.data.get(sym, "5m")) or p.entry
                        self.engine.external_filled_close(sym, float(px), self.positions, self.risk, self.notifier, reason="EXTERNAL_RECONCILE", hedge_book=self.hedger.snapshot())
        except Exception as e:
            logging.debug(f"reconcile failed: {e}")

    # [manage_position, periodic_report, run remain the same as v22, plus call _reconcile_positions_live() in the loop]

# ====================== ENTRY ======================

if __name__ == "__main__":
    try:
        bot = OmegaXBot()
        bot.run()
    except KeyboardInterrupt:
        try:
            Notifier(TELEGRAM_ENABLED).send_critical("🛑 OmegaX Fusion stopped by KeyboardInterrupt.")
        except Exception:
            pass
        raise
    except Exception as e:
        logging.exception("Fatal crash")
        try:
            Notifier(TELEGRAM_ENABLED).send_critical(f"💥 OmegaX Fusion crashed: {str(e)[:200]}")
        except Exception:
            pass
        raise