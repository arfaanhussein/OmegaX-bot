# -*- coding: utf-8 -*-
"""
OmegaX Futures Bot (Paper by default, LIVE-ready) - Thread-safe + Telegram lifecycle alerts
- Python 3.13.4
- Exchange: Binance USDT-M Futures (ccxt REST; python-binance user WS for fills)
- Concurrency safety:
  - Single-writer event queue: WS thread only enqueues; main thread mutates state
  - Global RLock guards all state reads/writes
  - Flask endpoints read snapshots under lock
- Execution:
  - Native SL/TP brackets at entry; cancel&replace on updates
  - Bracket repair loop + safety flatten
  - Sync live open orders on startup (restore bracket IDs)
  - WS fills reconciliation fixes cancel/replace race
- Strategy:
  - Bandit meta-allocator across alphas; closed-candle signals; continuous strengths
- Hedging:
  - Overlay hedge (beta) + per-trade temp hedges tracked separately; reconciled to a single BTC hedge
- Telegram:
  - Alerts: startup, health server up, WS started, loop errors (throttled), hard stop, shutdown (SIGINT/SIGTERM), crash

Env:
  MODE = paper|live
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID (recommended)
  BINANCE_API_KEY, BINANCE_API_SECRET (required in live)
  PORT (default 10000)
  LOG_LEVEL (default INFO)

pip install:
  ccxt python-binance pandas numpy ta Flask requests
"""
from __future__ import annotations

import os
import time
import json
import random
import logging
import threading
from threading import RLock
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Tuple
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
SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "ADA/USDT", "DOGE/USDT", "TRX/USDT", "AVAX/USDT", "LINK/USDT",
    "DOT/USDT", "MATIC/USDT", "BCH/USDT", "LTC/USDT", "NEAR/USDT",
    "FIL/USDT", "ATOM/USDT", "ICP/USDT", "APT/USDT", "ARB/USDT"
]
SYMBOL_ALIASES = {  # Binance futures rename
    "MATIC/USDT": "POL/USDT",
}
INDEX_HEDGE_SYMBOLS = ["BTC/USDT"]

TIMEFRAMES = ["5m", "15m", "1h"]
TF_SECONDS = {"5m": 300, "15m": 900, "1h": 3600}

MODE = os.environ.get("MODE", "paper").lower()

INITIAL_BALANCE = 3000.0
LEVERAGE_MIN = 10.0
LEVERAGE_MAX = 20.0

RISK_PER_TRADE = 0.005
MAX_TOTAL_RISK = 0.06
MAX_CONCURRENT_POS = 8
COOLDOWN_AFTER_LOSS_SEC = 600
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

RECOVERY_ENABLED = True
RECOVERY_BUCKET_FRACTION = 0.35
RECOVERY_MAX_RISK_FRAC = 0.012
RECOVERY_MIN_RISK_FRAC = 0.001
RECOVERY_SCALP_SL_ATR = 0.6
RECOVERY_SCALP_TP_ATR = 1.0
MAX_PARALLEL_RECOVERY_TRADES = 1

TEMP_ENTRY_HEDGE_FRAC = 0.3
TEMP_HEDGE_CLOSE_AT_R = 0.7

FUNDING_TTL = 600
HIGH_POSITIVE_FUNDING = 0.00025
HIGH_NEGATIVE_FUNDING = -0.00025

GLOBAL_LOOP_SLEEP_RANGE = (0.9, 1.6)
MAX_KLINE_UPDATES_PER_LOOP = 2
TICKER_FRESHNESS_SEC = 5.0
BACKOFF_BASE = 1.5
BACKOFF_MAX = 30.0
HTTP_TIMEOUT = 10

# --- FIX: Hardcoded for debugging - THIS IS INSECURE AND MUST BE REMOVED AFTER TESTING ---
TELEGRAM_TOKEN = "8384498061:AAElt7HeM88jfune948IcKkysHpw1tmXrlc"  # Use your NEW token
TELEGRAM_CHAT_ID = "1040990874"                                    # Use your verified Chat ID
# -----------------------------------------------------------------------------------------
TELEGRAM_ENABLED = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)
TELEGRAM_MIN_INTERVAL = 0.8

PORT = int(os.environ.get("PORT", "10000"))
STATE_FILE = "state.json"
# ====================================================

# ... (The rest of the file is identical to the last correct version) ...
# The full, correct code follows for completeness.

def setup_logging():
    lvl = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, lvl, logging.INFO),
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


class ExchangeWrapper:
    def __init__(self):
        self.ex = ccxt.binance({
            "apiKey": os.environ.get("BINANCE_API_KEY"),
            "secret": os.environ.get("BINANCE_API_SECRET"),
            "options": {"defaultType": "future"},
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
    def __init__(self, ex: ExchangeWrapper, symbols: List[str], timeframes: List[str]):
        self.ex, self.syms, self.tfs = ex, symbols, timeframes
        self.df: Dict[str, Dict[str, pd.DataFrame]] = {s: {tf: pd.DataFrame() for tf in timeframes} for s in symbols}
        self.last: Dict[str, Dict[str, float]] = {s: {tf: 0.0 for tf in timeframes} for s in symbols}

    def cold_start(self):
        for s in self.syms:
            for tf in self.tfs:
                self._update_one(s, tf)
                sleep_jitter(0.2, 0.5)

    def _update_one(self, s, tf):
        d = self.ex.fetch_ohlcv(s, tf, limit=300)
        if not d:
            return
        df = pd.DataFrame(d, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df = df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        self.df[s][tf] = df
        self.last[s][tf] = time.time()

    def schedule_updates(self, max_updates=MAX_KLINE_UPDATES_PER_LOOP) -> int:
        tasks = []
        now = time.time()
        for s in self.syms:
            for tf in self.tfs:
                due = self.last[s][tf] + TF_SECONDS[tf]
                if now - due >= -1.0:
                    tasks.append((now - due, s, tf))
        tasks.sort(reverse=True)
        cnt = 0
        for _, s, tf in tasks[:max_updates]:
            try:
                self._update_one(s, tf)
                cnt += 1
            except Exception as e:
                logging.warning(f"update {s} {tf} failed: {e}")
        return cnt

    def get(self, s, tf):
        return self.df[s][tf]


class Persistence:
    def __init__(self, path: str = STATE_FILE):
        self.path = path

    def load(self):
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"State load failed: {e}")
            return {}

    def save(self, risk, positions: Dict[str, Optional["Position"]], pending: List[dict] | None = None, hedge_book: dict | None = None):
        try:
            serial = {
                "balance": risk.balance,
                "equity_peak": risk.equity_peak,
                "daily_pnl": risk.daily_pnl,
                "loss_bucket": risk.loss_bucket,
                "cooldown": risk.cooldown,
                "paused_until": risk.paused_until,
                "risk_off_until": getattr(risk, "_risk_off_until", 0.0),
                "positions": {s: (asdict(p) if p else None) for s, p in positions.items()},
                "pending_intents": pending or [],
                "hedge_book": hedge_book or {},
                "ts": time.time()
            }
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(serial, f, indent=2)
            os.replace(tmp, self.path)
        except Exception as e:
            logging.warning(f"State save failed: {e}")


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

    def dir(self):
        return 1 if self.side == "long" else -1


class Mode:
    PAPER = "paper"
    LIVE = "live"


class HardStopTriggered(Exception):
    pass


class RiskManager:
    def __init__(self, equity: float):
        self.initial = equity
        self.balance = equity
        self.equity_peak = equity
        self.paused_until = 0.0
        self._risk_off_until = 0.0
        self.cooldown: Dict[str, float] = {}
        self.consec_losses = 0
        self.day_anchor = utc_midnight_ts()
        self.daily_pnl = 0.0
        self.loss_bucket = 0.0

    @property
    def drawdown(self) -> float:
        peak = max(self.equity_peak, self.balance)
        return (peak - self.balance) / peak if peak > 0 else 0.0

    @property
    def risk_off(self) -> bool:
        return time.time() < self._risk_off_until or time.time() < self.paused_until

    def roll_day_if_needed(self):
        now_midnight = utc_midnight_ts()
        if now_midnight > self.day_anchor:
            self.day_anchor = now_midnight
            self.daily_pnl = 0.0

    def note_open(self, sym: str):
        self.cooldown[sym] = time.time() + MIN_TRADE_COOLDOWN_SEC

    def can_trade(self, sym: str) -> bool:
        return time.time() >= self.cooldown.get(sym, 0.0)

    def update_on_close(self, pnl: float):
        before = self.balance
        self.balance += pnl
        self.equity_peak = max(self.equity_peak, self.balance)
        self.daily_pnl += (self.balance - before)
        if pnl < 0:
            self.consec_losses += 1
            self.loss_bucket += -pnl
        else:
            self.consec_losses = 0
            if self.loss_bucket > 0:
                self.loss_bucket = max(0.0, self.loss_bucket - pnl * 0.5)
        self._safety_checks()

    def _safety_checks(self):
        self.roll_day_if_needed()
        eq = self.balance
        if self.daily_pnl < -DAILY_MAX_LOSS_FRAC * eq:
            self._risk_off_until = max(self._risk_off_until, self.day_anchor + 86400)
            logging.warning("Risk-off until UTC midnight (daily loss).")
        if self.drawdown > SOFT_PAUSE_DD:
            self.paused_until = max(self.paused_until, time.time() + 1800)
            logging.warning(f"Soft pause: DD {self.drawdown*100:.2f}%")
        if self.drawdown > MAX_DRAWDOWN:
            raise HardStopTriggered("Hard drawdown exceeded")

    def dynamic_leverage(self, vol_spike: bool, alpha_boost: float) -> float:
        base = 14.0
        if vol_spike:
            base *= 0.75
        if self.consec_losses >= 2:
            base *= 0.85
        if self.drawdown > 0.02:
            base *= 0.9
        base *= (1.0 + np.clip(alpha_boost, -0.3, 0.3))
        return float(np.clip(round(base, 2), LEVERAGE_MIN, LEVERAGE_MAX))

    def risk_multiplier(self, vol_spike: bool, alpha_boost: float) -> float:
        mult = 1.0
        if self.consec_losses >= 2:
            mult *= 0.8
        if vol_spike:
            mult *= 0.75
        if self.risk_off:
            mult *= 0.5
        mult *= (1.0 + np.clip(alpha_boost, -0.3, 0.3))
        return float(np.clip(mult, 0.3, 1.6))

    def position_size_qty(self, entry: float, sl: float, base_risk_frac: float, open_positions: List[Position]) -> float:
        if entry <= 0 or sl <= 0 or entry == sl:
            return 0.0
        total_potential_loss = sum(abs(p.entry - p.sl) * p.qty for p in open_positions if not p.is_hedge)
        if total_potential_loss / max(1e-9, self.balance) >= MAX_TOTAL_RISK:
            return 0.0
        return max(self.balance * base_risk_frac / abs(entry - sl), 0.0)

    def recovery_risk_budget(self) -> float:
        if not RECOVERY_ENABLED or self.loss_bucket <= 0:
            return 0.0
        eq = self.balance
        allowed = min(self.loss_bucket * RECOVERY_BUCKET_FRACTION, eq * RECOVERY_MAX_RISK_FRAC)
        allowed = max(allowed, eq * RECOVERY_MIN_RISK_FRAC)
        return allowed


@dataclass
class SymbolAlpha:
    pnls: List[float] = field(default_factory=list)

    def record(self, pnl: float):
        self.pnls.append(pnl)
        self.pnls = self.pnls[-50:]

    def expectancy(self) -> float:
        if not self.pnls:
            return 0.0
        wins = [x for x in self.pnls if x >= 0]
        losses = [x for x in self.pnls if x < 0]
        wr = len(wins) / len(self.pnls)
        aw = np.mean(wins) if wins else 0.0
        al = np.mean(losses) if losses else 0.0
        return wr * aw + (1 - wr) * al

    def score(self) -> float:
        return float(np.clip(self.expectancy() / 30.0, -0.3, 0.3))


class AlphaEngine:
    def __init__(self, symbols: List[str]):
        self.stats = {s: SymbolAlpha() for s in symbols}

    def update(self, symbol: str, pnl: float):
        self.stats[symbol].record(pnl)

    def boost(self, symbol: str) -> float:
        return self.stats[symbol].score()


@dataclass
class Welford:
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, x: float):
        self.n += 1
        d = x - self.mean
        self.mean += d / self.n
        self.m2 += d * (x - self.mean)

    def variance(self) -> float:
        return self.m2 / (self.n - 1) if self.n > 1 else 1.0


class MetaPolicy:
    ALPHAS = ["trend", "meanrev", "vwap", "donchian", "pair", "xsmom", "carry"]

    def __init__(self, symbols: List[str]):
        self.stats = {s: {a: Welford() for a in self.ALPHAS} for s in symbols}

    def sample_weights(self, symbol: str) -> Dict[str, float]:
        out = {}
        for a, w in self.stats[symbol].items():
            std = np.sqrt(abs(w.variance())) if w.n > 1 else 1.0
            noise = std / np.sqrt(max(1, w.n))
            out[a] = random.gauss(w.mean, max(1e-3, noise))
        vals = np.array(list(out.values()))
        vals -= np.max(vals)
        ws = np.exp(vals)
        denom = np.sum(ws)
        if denom > 0:
            ws /= denom
        return {a: float(ws[i]) for i, a in enumerate(out.keys())}

    def update(self, symbol: str, alpha_tag: str, r_multiple: float):
        if alpha_tag in self.ALPHAS:
            self.stats[symbol][alpha_tag].update(r_multiple)


def sig01(x: float) -> float:
    try:
        return float(1.0 / (1.0 + np.exp(-x)))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def clamp01(x: float) -> float:
    return float(max(0.0, min(1.0, x)))


class Strategy:
    def detect_regime(self, df5: pd.DataFrame, df15: pd.DataFrame) -> Tuple[str, bool]:
        if df5.empty or df15.empty:
            return "Unknown", False
        adx = ADXIndicator(df5["high"], df5["low"], df5["close"], 14).adx().iloc[-1]
        atr = AverageTrueRange(df5["high"], df5["low"], df5["close"], 14).average_true_range()
        atr_now = float(atr.iloc[-1])
        atr_avg = float(atr.tail(100).mean()) if len(atr) >= 20 else atr_now
        vol_spike = atr_now > atr_avg * 2.2 if atr_avg > 0 else False
        if adx > 30 and not vol_spike:
            return "Strong Trending", vol_spike
        if adx > 20 and vol_spike:
            return "Volatile Trending", vol_spike
        if vol_spike:
            return "High Volatility", vol_spike
        return "Ranging/Calm", vol_spike

    def features(self, df5, df15, df1h):
        ema_f = EMAIndicator(df5["close"], 12).ema_indicator()
        ema_s = EMAIndicator(df5["close"], 26).ema_indicator()
        macd_o = MACD(df5["close"])
        macd = macd_o.macd()
        macd_s = macd_o.macd_signal()
        rsi = RSIIndicator(df5["close"], 14).rsi()
        bb = BollingerBands(df5["close"], 20, 2)
        bb_u = bb.bollinger_hband()
        bb_l = bb.bollinger_lband()
        atr = AverageTrueRange(df5["high"], df5["low"], df5["close"], 14).average_true_range()
        st_k = StochasticOscillator(df5["high"], df5["low"], df5["close"]).stoch()
        vwap = VolumeWeightedAveragePrice(df5["high"], df5["low"], df5["close"], df5["volume"]).volume_weighted_average_price()
        sma50_1h = SMAIndicator(df1h["close"], 50).sma_indicator()
        don_hi = df5["high"].rolling(20).max()
        don_lo = df5["low"].rolling(20).min()
        ichi = ((df15["high"] + df15["low"]) / 2).rolling(26).mean()
        return dict(ema_f=ema_f, ema_s=ema_s, macd=macd, macd_s=macd_s, rsi=rsi,
                    bb_u=bb_u, bb_l=bb_l, atr=atr, st_k=st_k, vwap=vwap,
                    sma50_1h=sma50_1h, don_hi=don_hi, don_lo=don_lo, ichi=ichi)

    def slope_dir(self, closes: pd.Series) -> Optional[str]:
        n = min(80, len(closes))
        if n < 20:
            return None
        y = closes.iloc[-n:].values
        x = np.arange(n)
        slope = np.polyfit(x, y, 1)[0]
        return "up" if slope > 0 else "down"

    def cross_sectional_mom_rank(self, df1h_map: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        rets = {}
        for s, df1h in df1h_map.items():
            if df1h.empty or len(df1h) < 50:
                continue
            rets[s] = df1h["close"].iloc[-1] / df1h["close"].iloc[-50] - 1.0
        if not rets:
            return {}
        vals = np.array(list(rets.values()))
        z = (vals - vals.mean()) / (vals.std() + 1e-9)
        return {sym: float(z[i]) for i, sym in enumerate(rets.keys())}

    def pair_residual_side(self, sym_df: pd.DataFrame, btc_df: pd.DataFrame) -> Optional[Tuple[str, float]]:
        if sym_df.empty or btc_df.empty:
            return None
        rs = sym_df["close"].pct_change().dropna().tail(300)
        rb = btc_df["close"].pct_change().dropna().tail(300)
        n = min(len(rs), len(rb))
        if n < 120:
            return None
        rs, rb = rs.iloc[-n:], rb.iloc[-n:]
        cov = np.cov(rs, rb)[0, 1]
        varb = np.var(rb)
        beta = cov / varb if varb > 0 else 1.0
        spread = rs - beta * rb
        z = (spread - spread.mean()) / (spread.std() + 1e-9)
        zl = float(z.iloc[-1])
        if zl > 2.0:
            return "short", min(1.0, (zl - 2.0) / 2.0)
        if zl < -2.0:
            return "long", min(1.0, (-2.0 - zl) / 2.0)
        return None

    def strength_trend(self, ema_f, ema_s, macd, macd_s, px, atr):
        ema_gap = (ema_f - ema_s) / (atr + 1e-9)
        macd_gap = (macd - macd_s) / (atr + 1e-9)
        return sig01(0.7 * ema_gap + 0.3 * macd_gap), sig01(0.7 * (-ema_gap) + 0.3 * (-macd_gap))

    def strength_meanrev(self, px, bb_u, bb_l, rsi, st_k, atr):
        band_mid = (bb_u + bb_l) / 2.0
        half = (bb_u - bb_l) / 2.0 + 1e-9
        z = (px - band_mid) / half
        rsi_bias = (50 - rsi) / 25.0
        st_bias = (50 - st_k) / 25.0
        return sig01(-z + 0.2 * rsi_bias + 0.1 * st_bias), sig01(+z - 0.2 * rsi_bias - 0.1 * st_bias)

    def strength_vwap(self, px, vwap, macd, atr):
        gap = (px - vwap) / (atr + 1e-9)
        macd_sign = np.tanh(macd / (atr + 1e-9))
        return sig01(gap + 0.2 * macd_sign), sig01(-gap - 0.2 * macd_sign)

    def strength_donchian(self, px, don_hi, don_lo, atr):
        return clamp01((px - don_hi) / (atr + 1e-9)), clamp01((don_lo - px) / (atr + 1e-9))
    
    def alpha_scores(self, symbol, df5, df15, df1h, btc_df, cs_rank, funding_rate):
        f = self.features(df5, df15, df1h)
        px = float(df5["close"].iloc[-1])
        last = float(df5["close"].iloc[-2]) if len(df5) >= 2 else px
        change = (px - last) / last if last else 0.0
        if abs(change) > 0.08:
            return {}
        atr = float(f["atr"].iloc[-1]) if not np.isnan(f["atr"].iloc[-1]) else max(1e-8, float(df5["close"].diff().abs().tail(14).mean()))

        lt, st = self.strength_trend(f["ema_f"].iloc[-1], f["ema_s"].iloc[-1], f["macd"].iloc[-1], f["macd_s"].iloc[-1], px, atr)
        lmr, smr = self.strength_meanrev(px, f["bb_u"].iloc[-1], f["bb_l"].iloc[-1], f["rsi"].iloc[-1], f["st_k"].iloc[-1], atr)
        lvm, svm = self.strength_vwap(px, f["vwap"].iloc[-1], f["macd"].iloc[-1], atr)
        ld, sd = self.strength_donchian(px, f["don_hi"].iloc[-1], f["don_lo"].iloc[-1], atr)

        pr = self.pair_residual_side(df5, btc_df)
        if pr:
            side, mag = pr
            lp, sp = (mag, 0.0) if side == "long" else (0.0, mag)
        else:
            lp, sp = 0.0, 0.0

        cs_mag = float(np.clip(abs(cs_rank) / 2.5, 0.0, 1.0))
        lxs, sxs = (cs_mag, 0.0) if cs_rank > 0 else (0.0, cs_mag)

        cr_l = 0.0
        cr_s = 0.0
        if funding_rate is not None:
            if funding_rate > HIGH_POSITIVE_FUNDING:
                cr_s = float(np.clip((funding_rate - HIGH_POSITIVE_FUNDING) * 4000, 0.0, 1.0))
            elif funding_rate < HIGH_NEGATIVE_FUNDING:
                cr_l = float(np.clip((HIGH_NEGATIVE_FUNDING - funding_rate) * 4000, 0.0, 1.0))

        return {
            "trend": (lt, st),
            "meanrev": (lmr, smr),
            "vwap": (lvm, svm),
            "donchian": (ld, sd),
            "pair": (lp, sp),
            "xsmom": (lxs, sxs),
            "carry": (cr_l, cr_s),
        }


class PortfolioManager:
    def __init__(self, bot: "OmegaXBot"):
        self.bot = bot

    def exposure(self) -> Dict[str, float]:
        with self.bot.lock:
            items = list(self.bot.positions.items())
        res: Dict[str, float] = {}
        for _, p in items:
            if not p or p.is_hedge:
                continue
            price = self.bot.prices.get(p.symbol, self.bot.data.get(p.symbol, "5m"))
            if not price:
                continue
            res[p.symbol] = res.get(p.symbol, 0.0) + p.dir() * p.qty * price
        return res

    def bucket_violation(self) -> Optional[str]:
        majors = {"BTC/USDT", "ETH/USDT", "BNB/USDT"}
        with self.bot.lock:
            bal = self.bot.risk.balance
        e = self.exposure()
        majors_exp = sum(v for s, v in e.items() if s in majors)
        alts_exp = sum(v for s, v in e.items() if s not in majors)
        if abs(majors_exp) > 0.65 * bal:
            return "majors"
        if abs(alts_exp) > 0.55 * bal:
            return "alts"
        return None


@dataclass
class OrderEvent:
    symbol: str
    ord_type: str
    status: str
    side: str
    order_id: str
    avg_price: float
    reduce_only: bool


class ExecutionEngine:
    def __init__(self, mode: str, ex_wrapper: Optional[ExchangeWrapper], persistence: Persistence, notifier: Notifier, lock: RLock):
        self.mode = mode
        self.exw = ex_wrapper
        self.persistence = persistence
        self.notifier = notifier
        self.lock = lock
        self.pending_intents: List[dict] = []
        self.repair_tasks: Dict[str, dict] = {}

    def _persist(self, risk, positions, hedge_book=None):
        with self.lock:
            self.persistence.save(risk, positions, pending=self.pending_intents, hedge_book=hedge_book or {})

    def _parse_fill(self, order) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        if not order:
            return None, None, None
        avg = order.get("average") or order.get("price")
        if avg is None and "info" in order:
            info = order["info"]
            avg = info.get("avgPrice") or info.get("ap")
        if avg is None and order.get("cost") and order.get("filled"):
            try:
                avg = float(order["cost"]) / float(order["filled"])
            except Exception:
                avg = None
        order_id = order.get("id") or (order.get("info", {}) or {}).get("orderId")
        return (float(avg) if avg is not None else None,
                float(order.get("filled")) if order.get("filled") is not None else None,
                str(order_id) if order_id is not None else None)

    def _opposite(self, side: str) -> str:
        return "sell" if side == "long" else "buy"

    def _place_brackets(self, symbol, side, qty, sl, tp) -> Tuple[Optional[str], Optional[str]]:
        if self.mode == Mode.PAPER:
            return "SL-PAPER", "TP-PAPER"
        sl_id = None
        tp_id = None
        if sl and sl > 0:
            try:
                res_sl = self.exw.ex.create_order(
                    symbol, type="STOP_MARKET", side=self._opposite(side), amount=qty,
                    params={"stopPrice": sl, "reduceOnly": True}
                )
                _, _, sl_id = self._parse_fill(res_sl)
            except Exception as e:
                logging.warning(f"SL place failed {symbol}: {e}")
        if tp and tp > 0:
            try:
                res_tp = self.exw.ex.create_order(
                    symbol, type="TAKE_PROFIT_MARKET", side=self._opposite(side), amount=qty,
                    params={"stopPrice": tp, "reduceOnly": True}
                )
                _, _, tp_id = self._parse_fill(res_tp)
            except Exception as e:
                logging.warning(f"TP place failed {symbol}: {e}")
        return sl_id, tp_id

    def _cancel_brackets(self, symbol, pos: "Position"):
        if self.mode == Mode.PAPER:
            pos.sl_order_id = None
            pos.tp_order_id = None
            return
        if pos.sl_order_id:
            try:
                self.exw.cancel_order_safe(pos.sl_order_id, symbol)
            except Exception:
                pass
            pos.sl_order_id = None
        if pos.tp_order_id:
            try:
                self.exw.cancel_order_safe(pos.tp_order_id, symbol)
            except Exception:
                pass
            pos.tp_order_id = None

    def open(self, intent, positions, risk, notifier, hedge_book=None) -> bool:
        symbol, side, qty, entry, sl, tp, lev, mode, tag = intent
        now = time.time()
        if self.mode == Mode.PAPER:
            with self.lock:
                positions[symbol] = Position(symbol, side, qty, entry, sl, tp, lev, now, mode=mode, tag=tag)
            notifier.send(f"{symbol}: 🚀 OPEN {mode}/{tag} {side.upper()} qty={qty:.6f} @ {entry:.4f} SL={sl:.4f} TP={tp:.4f} lev={lev}x")
            self._persist(risk, positions, hedge_book)
            return True
        try:
            client_id = f"ox-{int(time.time()*1000)}-{random.randint(100,999)}"
            with self.lock:
                self.pending_intents.append({
                    "symbol": symbol, "side": side, "qty": qty, "sl": sl, "tp": tp,
                    "lev": lev, "mode": mode, "tag": tag, "clientOrderId": client_id, "ts": now
                })
            self._persist(risk, positions, hedge_book)
            order = self.exw.ex.create_order(
                symbol, type="market", side=("buy" if side == "long" else "sell"),
                amount=qty, params={"newClientOrderId": client_id}
            )
            avg, filled, order_id = self._parse_fill(order)
            avg = avg or entry
            q = filled or qty
            with self.lock:
                positions[symbol] = Position(symbol, side, q, float(avg), sl, tp, lev, now, mode=mode, tag=tag,
                                             entry_order_id=order_id, client_order_id=client_id)
            if mode in ("hedge", "temp_hedge"):
                notifier.send(f"{symbol}: ✅ LIVE OPEN {mode}/{tag} {side.upper()} qty={q:.6f} @ {avg:.4f}")
            else:
                sl_id, tp_id = self._place_brackets(symbol, side, q, sl, tp)
                with self.lock:
                    pos = positions.get(symbol)
                    if pos:
                        pos.sl_order_id = sl_id
                        pos.tp_order_id = tp_id
                if not sl_id or not tp_id:
                    with self.lock:
                        self.repair_tasks[symbol] = {
                            "side": side, "qty": q, "sl": sl, "tp": tp, "attempts": 0, "next_try": time.time() + 3
                        }
                    notifier.send(f"{symbol}: ⚠️ Bracket placement incomplete. Repair scheduled.")
                notifier.send(f"{symbol}: ✅ LIVE OPEN {mode}/{tag} {side.upper()} qty={q:.6f} @ {avg:.4f} SL={sl:.4f} TP={tp:.4f} lev={lev}x")
            with self.lock:
                self.pending_intents = [p for p in self.pending_intents if p.get("clientOrderId") != client_id]
            self._persist(risk, positions, hedge_book)
            return True
        except Exception as e:
            logging.error(f"LIVE open failed {symbol}: {e}")
            return False

    def replace_brackets(self, symbol: str, positions, new_qty: float, new_sl: float, new_tp: float, risk, notifier, hedge_book=None):
        with self.lock:
            p = positions.get(symbol)
        if not p:
            return
        if p.mode in ("hedge", "temp_hedge"):
            return
        with self.lock:
            p.sl = new_sl
            p.tp = new_tp
            self._cancel_brackets(symbol, p)
        if self.mode == Mode.PAPER:
            with self.lock:
                p.sl_order_id = "SL-PAPER"
                p.tp_order_id = "TP-PAPER"
            self._persist(risk, positions, hedge_book)
            return
        sl_id, tp_id = self._place_brackets(symbol, p.side, new_qty, new_sl, new_tp)
        with self.lock:
            pos2 = positions.get(symbol)
            if not pos2:
                return
            pos2.sl_order_id = sl_id
            pos2.tp_order_id = tp_id
            if not sl_id or not tp_id:
                self.repair_tasks[symbol] = {"side": pos2.side, "qty": new_qty, "sl": new_sl, "tp": new_tp, "attempts": 0, "next_try": time.time() + 3}
        if not sl_id or not tp_id:
            notifier.send(f"{symbol}: ⚠️ Bracket re-arm incomplete. Repair scheduled.")
        self._persist(risk, positions, hedge_book)

    def partial_close(self, symbol: str, close_qty: float, positions, risk, notifier, hedge_book=None) -> Optional[float]:
        with self.lock:
            p = positions.get(symbol)
        if not p or close_qty <= 0:
            return None
        if close_qty > p.qty:
            close_qty = p.qty
        if self.mode == Mode.PAPER:
            return None
        try:
            res = self.exw.ex.create_order(symbol, type="market", side=self._opposite(p.side), amount=close_qty, params={"reduceOnly": True})
            avg, filled, _ = self._parse_fill(res)
            fill_qty = filled or close_qty
            t = self.exw.fetch_ticker(symbol)
            fill_price = avg or t.get("last") or t.get("close")
            pnl_part = p.dir() * fill_qty * (float(fill_price) - p.entry)
            with self.lock:
                pos2 = positions.get(symbol)
                if not pos2:
                    return pnl_part
                pos2.qty -= fill_qty
                risk.update_on_close(pnl_part)
            notifier.send(f"{symbol}: ↘️ Partial {fill_qty:.6f} @ {float(fill_price):.4f} pnl=${pnl_part:.2f}")
            with self.lock:
                still = positions.get(symbol)
            if still and still.qty > 0:
                self.replace_brackets(symbol, positions, still.qty, still.sl, still.tp, risk, notifier, hedge_book)
            else:
                with self.lock:
                    positions[symbol] = None
            return pnl_part
        except Exception as e:
            logging.error(f"Partial close LIVE failed {symbol}: {e}")
            return None

    def add_size(self, symbol: str, add_qty: float, positions, risk, notifier, hedge_book=None):
        with self.lock:
            p = positions.get(symbol)
        if not p or add_qty <= 0:
            return
        if self.mode == Mode.PAPER:
            return
        try:
            res = self.exw.ex.create_order(symbol, type="market", side=("buy" if p.side == "long" else "sell"), amount=add_qty)
            avg, filled, _ = self._parse_fill(res)
            fill_qty = filled or add_qty
            t = self.exw.fetch_ticker(symbol)
            fill_price = avg or t.get("last") or t.get("close")
            with self.lock:
                pos2 = positions.get(symbol)
                if not pos2:
                    return
                new_qty = pos2.qty + fill_qty
                pos2.entry = (pos2.entry * pos2.qty + float(fill_price) * fill_qty) / max(1e-9, new_qty)
                pos2.qty = new_qty
            notifier.send(f"{symbol}: ➕ Added {fill_qty:.6f} @ {float(fill_price):.4f}; new qty={new_qty:.6f}, avg={pos2.entry:.4f}")
            self.replace_brackets(symbol, positions, pos2.qty, pos2.sl, pos2.tp, risk, notifier, hedge_book)
        except Exception as e:
            logging.error(f"Add size LIVE failed {symbol}: {e}")

    def tick_repair(self, positions, risk, notifier, hedge_book=None, max_attempts: int = 6):
        now = time.time()
        with self.lock:
            tasks = list(self.repair_tasks.items())
        for sym, task in tasks:
            if now < task["next_try"]:
                continue
            with self.lock:
                p = positions.get(sym)
            if not p:
                with self.lock:
                    self.repair_tasks.pop(sym, None)
                continue
            if p.mode in ("hedge", "temp_hedge"):
                with self.lock:
                    self.repair_tasks.pop(sym, None)
                continue
            sl_id, tp_id = self._place_brackets(sym, task["side"], task["qty"], task["sl"], task["tp"])
            with self.lock:
                pos2 = positions.get(sym)
                if not pos2:
                    self.repair_tasks.pop(sym, None)
                    continue
                if sl_id:
                    pos2.sl_order_id = sl_id
                if tp_id:
                    pos2.tp_order_id = tp_id
                ok = bool(pos2.sl_order_id and pos2.tp_order_id)
                if ok:
                    self.repair_tasks.pop(sym, None)
            if ok:
                notifier.send(f"{sym}: ✅ Brackets repaired.")
                self._persist(risk, positions, hedge_book)
                continue
            with self.lock:
                task_ref = self.repair_tasks.get(sym)
                if not task_ref:
                    continue
                task_ref["attempts"] += 1
                task_ref["next_try"] = now + min(10, 2 + task_ref["attempts"] * 2)
                attempts = task_ref["attempts"]
            if attempts >= max_attempts:
                notifier.send(f"{sym}: ❗ Brackets failed after {attempts} tries. Flattening.")
                t = self.exw.fetch_ticker(sym)
                px = t.get("last") or t.get("close")
                if px:
                    self.close(sym, positions, float(px), "NO_BRACKETS_SAFETY", risk, notifier, hedge_book)

    def sync_orders(self, positions):
        try:
            open_orders = self.exw.fetch_open_orders()
        except Exception as e:
            logging.warning(f"open orders sync failed: {e}")
            return
        by_sym: Dict[str, List[dict]] = {}
        for o in open_orders:
            by_sym.setdefault(o.get("symbol", ""), []).append(o)
        with self.lock:
            items = list(positions.items())
        for sym, pos in items:
            if not pos or sym not in by_sym:
                continue
            for o in by_sym[sym]:
                typ = str(o.get("type", "")).upper()
                side = str(o.get("side", "")).lower()
                info = o.get("info", {}) or {}
                reduce_only = o.get("reduceOnly")
                if reduce_only is None:
                    reduce_only = str(info.get("reduceOnly", "false")).lower() == "true"
                if not reduce_only:
                    continue
                stop_price = o.get("stopPrice") or info.get("stopPrice") or o.get("price")
                try:
                    sp = float(stop_price) if stop_price is not None else None
                except Exception:
                    sp = None
                if pos.side == "long" and side != "sell":
                    continue
                if pos.side == "short" and side != "buy":
                    continue
                with self.lock:
                    p2 = positions.get(sym)
                    if not p2:
                        continue
                    if typ in ("STOP", "STOP_MARKET") and sp:
                        p2.sl_order_id = o.get("id") or info.get("orderId")
                        if not p2.sl:
                            p2.sl = sp
                    if typ in ("TAKE_PROFIT", "TAKE_PROFIT_MARKET") and sp:
                        p2.tp_order_id = o.get("id") or info.get("orderId")
                        if not p2.tp:
                            p2.tp = sp

    def close(self, symbol, positions, price, reason, risk, notifier, hedge_book=None):
        with self.lock:
            p = positions.get(symbol)
        if not p:
            return
        pnl = p.dir() * p.qty * (price - p.entry)
        if self.mode == Mode.LIVE:
            try:
                with self.lock:
                    p2 = positions.get(symbol)
                    if p2:
                        self._cancel_brackets(symbol, p2)
                self.exw.ex.create_order(symbol, type="market", side=self._opposite(p.side), amount=p.qty, params={"reduceOnly": True})
            except Exception as e:
                logging.error(f"LIVE close failed {symbol}: {e}")
        with self.lock:
            positions[symbol] = None
            risk.update_on_close(pnl)
        notifier.send(f"{symbol}: ❌ CLOSE {reason} pnl=${pnl:.2f} equity=${risk.balance:.2f}")
        self._persist(risk, positions, hedge_book)

    def external_filled_close(self, symbol, fill_price: float, positions, risk, notifier, reason="BRACKET_FILLED", hedge_book=None):
        with self.lock:
            p = positions.get(symbol)
        if not p:
            return
        pnl = p.dir() * p.qty * (fill_price - p.entry)
        with self.lock:
            positions[symbol] = None
            risk.update_on_close(pnl)
        notifier.send(f"{symbol}: ✅ {reason} @ {fill_price:.4f} pnl=${pnl:.2f} equity=${risk.balance:.2f}")
        self._persist(risk, positions, hedge_book)

    def flatten_all(self, positions, prices: Dict[str, float], risk, notifier, hedge_book=None):
        with self.lock:
            syms = [s for s, p in positions.items() if p]
        for s in syms:
            px = prices.get(s)
            if not px:
                continue
            self.close(s, positions, px, "EMERGENCY_FLATTEN", risk, notifier, hedge_book)


class FundingCache:
    def __init__(self, ex: ExchangeWrapper):
        self.ex = ex
        self.last = 0.0
        self.rates = {}
        self._idx = 0

    def maybe_update(self):
        now = time.time()
        if now - self.last < FUNDING_TTL:
            return
        try:
            if hasattr(self.ex.ex, "fetchFundingRates"):
                bulk = self.ex.ex.fetchFundingRates(SYMBOLS)
                if isinstance(bulk, dict):
                    for sym, fr in bulk.items():
                        r = fr.get("fundingRate") or fr.get("info", {}).get("lastFundingRate")
                        if r is not None:
                            self.rates[sym] = float(r)
                self.last = now
                return
        except Exception:
            pass
        s = SYMBOLS[self._idx]
        self._idx = (self._idx + 1) % len(SYMBOLS)
        try:
            if hasattr(self.ex.ex, "fetchFundingRate"):
                fr = self.ex.ex.fetchFundingRate(s)
                if fr:
                    r = fr.get("fundingRate") or fr.get("info", {}).get("lastFundingRate")
                    if r is not None:
                        self.rates[s] = float(r)
        except Exception as e:
            logging.debug(f"funding fetch fail {s}: {e}")
        self.last = now


class HedgeEngine:
    def __init__(self, bot: "OmegaXBot"):
        self.bot = bot
        self.overlay_qty_target: float = 0.0
        self.temp_qty_by_underlier: Dict[str, float] = {}
        self.last_rebalance = 0.0

    def _beta(self, sym: str, base: str = "BTC/USDT") -> float:
        df_s = self.bot.data.get(sym, "5m")
        df_b = self.bot.data.get(base, "5m")
        if df_s.empty or df_b.empty:
            return 1.0
        rs = df_s["close"].pct_change().dropna().tail(400)
        rb = df_b["close"].pct_change().dropna().tail(400)
        n = min(len(rs), len(rb))
        if n < 120:
            return 1.0
        rs, rb = rs.iloc[-n:], rb.iloc[-n:]
        cov = np.cov(rs, rb)[0, 1]
        varb = np.var(rb)
        return float(np.clip(cov / varb if varb > 0 else 1.0, -2.0, 2.0))

    def compute_overlay_target(self):
        with self.bot.lock:
            items = list(self.bot.positions.items())
        net = 0.0
        for _, p in items:
            if not p or p.is_hedge or p.mode == "recovery":
                continue
            price = self.bot.prices.get(p.symbol, self.bot.data.get(p.symbol, "5m"))
            if not price:
                continue
            net += self._beta(p.symbol) * p.dir() * p.qty * price
        dd = self.bot.risk.drawdown
        ratio = 0.6 + min(0.4, dd * 2.0)
        if self.bot.risk.risk_off:
            ratio = max(ratio, 1.0)
        pb = self.bot.prices.get("BTC/USDT", self.bot.data.get("BTC/USDT", "5m"))
        if pb:
            self.overlay_qty_target = (-ratio * net) / pb

    def open_temp_hedge_for(self, underlier_sym: str, main_side: str, main_qty: float, main_entry: float):
        pb = self.bot.prices.get("BTC/USDT", self.bot.data.get("BTC/USDT", "5m"))
        if not pb:
            return
        notional = main_qty * main_entry * TEMP_ENTRY_HEDGE_FRAC
        qty_b = notional / pb
        delta = -qty_b if main_side == "long" else qty_b
        current = self.temp_qty_by_underlier.get(underlier_sym, 0.0)
        self.temp_qty_by_underlier[underlier_sym] = current + delta
        self.bot.notifier.send(f"🧩 Temp hedge delta for {underlier_sym}: {self.temp_qty_by_underlier[underlier_sym]:.6f}")
        self.rebalance()

    def close_temp_hedge_for(self, underlier_sym: str):
        if underlier_sym in self.temp_qty_by_underlier:
            self.temp_qty_by_underlier[underlier_sym] = 0.0
            self.bot.notifier.send(f"🧩 Temp hedge cleared for {underlier_sym}")
            self.rebalance()

    def target_net_qty(self) -> float:
        return self.overlay_qty_target + sum(self.temp_qty_by_underlier.values())

    def current_hedge_qty(self) -> float:
        with self.bot.lock:
            p = self.bot.positions.get("BTC/USDT")
        if p and p.is_hedge:
            return p.dir() * p.qty
        return 0.0

    def rebalance(self):
        if time.time() - self.last_rebalance < 60:
            return
        self.compute_overlay_target()
        target = self.target_net_qty()
        cur = self.current_hedge_qty()
        delta = target - cur
        sym = "BTC/USDT"
        price_b = self.bot.prices.get(sym, self.bot.data.get(sym, "5m"))
        if price_b is None:
            self.last_rebalance = time.time()
            return
        with self.bot.lock:
            p = self.bot.positions.get(sym)

        if abs(delta) < 1e-9:
            self.last_rebalance = time.time()
            return

        if not p or not p.is_hedge:
            side = "long" if delta > 0 else "short"
            intent = (sym, side, abs(delta), price_b, 0.0, 0.0, 1.0, "hedge", "overlay")
            opened = self.bot.engine.open(intent, self.bot.positions, self.bot.risk, self.bot.notifier, hedge_book=self.snapshot())
            if opened:
                with self.bot.lock:
                    pos = self.bot.positions.get(sym)
                    if pos:
                        pos.is_hedge = True
                        pos.mode = "hedge"
                        pos.tag = "hedge"
            self.last_rebalance = time.time()
            self.bot.persistence.save(self.bot.risk, self.bot.positions, pending=self.bot.engine.pending_intents, hedge_book=self.snapshot())
            return

        sign_cur = 1 if p.side == "long" else -1
        desired_sign = 1 if delta > 0 else -1
        if desired_sign == sign_cur:
            self.bot.engine.add_size(sym, abs(delta), self.bot.positions, self.bot.risk, self.bot.notifier, hedge_book=self.snapshot())
        else:
            if abs(delta) <= p.qty:
                self.bot.engine.partial_close(sym, abs(delta), self.bot.positions, self.bot.risk, self.bot.notifier, hedge_book=self.snapshot())
            else:
                remainder = abs(delta) - p.qty
                self.bot.engine.partial_close(sym, p.qty, self.bot.positions, self.bot.risk, self.bot.notifier, hedge_book=self.snapshot())
                side2 = "long" if desired_sign > 0 else "short"
                intent2 = (sym, side2, remainder, price_b, 0.0, 0.0, 1.0, "hedge", "overlay")
                opened2 = self.bot.engine.open(intent2, self.bot.positions, self.bot.risk, self.bot.notifier, hedge_book=self.snapshot())
                if opened2:
                    with self.bot.lock:
                        pos2 = self.bot.positions.get(sym)
                        if pos2:
                            pos2.is_hedge = True
                            pos2.mode = "hedge"
                            pos2.tag = "hedge"

        self.last_rebalance = time.time()
        self.bot.persistence.save(self.bot.risk, self.bot.positions, pending=self.bot.engine.pending_intents, hedge_book=self.snapshot())

    def snapshot(self) -> dict:
        return {"overlay_qty_target": self.overlay_qty_target, "temp_qty_by_underlier": self.temp_qty_by_underlier}


class UserStream:
    def __init__(self, api_key: str, api_secret: str, exwrap: ExchangeWrapper, enqueue_event):
        self.api_key = api_key
        self.api_secret = api_secret
        self.exwrap = exwrap
        self.enqueue_event = enqueue_event
        self.twm: Optional[ThreadedWebsocketManager] = None

    def start(self):
        if not BINANCE_WS_AVAILABLE:
            logging.warning("UserStream not started: python-binance not installed.")
            return
        if not self.api_key or not self.api_secret:
            logging.warning("UserStream not started: API keys missing.")
            return
        try:
            self.twm = ThreadedWebsocketManager(api_key=self.api_key, api_secret=self.api_secret)
            self.twm.start()
            self.twm.start_futures_user_socket(callback=self._handler)
            logging.info("UserStream started.")
        except Exception as e:
            logging.warning(f"UserStream start failed: {e}")

    def stop(self):
        try:
            if self.twm:
                self.twm.stop()
        except Exception:
            pass

    def _handler(self, msg: dict):
        try:
            if msg.get('e') != 'ORDER_TRADE_UPDATE':
                return
            o = msg.get('o', {})
            market_id = o.get('s')
            symbol = self.exwrap.id_to_symbol(market_id) or f"{market_id[:-4]}/{market_id[-4:]}"
            ord_type = o.get('o', '')
            status = o.get('X', '')
            side = o.get('S', '')
            order_id = str(o.get('i') or o.get('orderId'))
            last_fill = float(o.get('L') or 0.0)
            avg_price = float(o.get('ap') or last_fill or 0.0)
            reduce_only = str(o.get('R', 'false')).lower() == 'true' or str(o.get('r', 'false')).lower() == 'true'
            ev = OrderEvent(symbol, ord_type, status, side, order_id, avg_price, reduce_only)
            self.enqueue_event(ev)
        except Exception as e:
            logging.debug(f"UserStream handler error: {e}")


class OmegaXBot:
    def __init__(self):
        setup_logging()
        self.notifier = Notifier(TELEGRAM_ENABLED)
        self.notifier.send_critical(f"🚀 Starting OmegaX ({MODE}) with {len(SYMBOLS)} configured symbols.")

        self.ex = ExchangeWrapper()

        markets = self.ex.ex.load_markets()
        normalized = []
        for s in SYMBOLS:
            s2 = SYMBOL_ALIASES.get(s, s)
            if s2 in markets:
                normalized.append(s2)
            else:
                logging.warning(f"Skipping {s}: not found on Binance futures.")
        self.symbols = normalized

        self.data = DataFeed(self.ex, self.symbols, TIMEFRAMES)
        self.prices = PriceCache(self.ex, self.symbols)
        self.strategy = Strategy()
        self.risk = RiskManager(INITIAL_BALANCE)
        self.alpha = AlphaEngine(self.symbols)
        self.meta = MetaPolicy(self.symbols)
        self.funding = FundingCache(self.ex)
        self.persistence = Persistence()
        self.positions: Dict[str, Optional[Position]] = {s: None for s in self.symbols}
        self.lock = RLock()
        self.portfolio_manager = PortfolioManager(self)
        self.engine = ExecutionEngine(Mode.PAPER if MODE == "paper" else Mode.LIVE, self.ex, self.persistence, self.notifier, self.lock)
        self.hedger = HedgeEngine(self)
        self.last_report = 0.0
        self._last_loop_error_notify = 0.0

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

        self.notifier.send(f"✅ OmegaX online ({MODE}). Active symbols: {len(self.symbols)}. Equity ${self.risk.balance:.2f}")

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

    def enqueue_event(self, ev: OrderEvent):
        self.events.put(ev)

    def _drain_events(self):
        while True:
            try:
                ev = self.events.get_nowait()
            except Empty:
                break
            if ev.status != "FILLED":
                continue
            is_bracket = ev.ord_type.upper() in ("STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET") or ev.reduce_only
            if not is_bracket:
                continue
            fill = ev.avg_price if ev.avg_price > 0 else (self.prices.get(ev.symbol, self.data.get(ev.symbol, "5m")) or 0.0)
            if fill:
                self.engine.external_filled_close(ev.symbol, float(fill), self.positions, self.risk, self.notifier, reason=f"{ev.ord_type}_FILLED", hedge_book=self.hedger.snapshot())

    def _start_health_server(self):
        app = Flask(__name__)

        @app.route("/", methods=["GET", "HEAD"])
        def index():
            return "OmegaX bot running. Endpoints: /health, /metrics, /ping", 200

        @app.route("/ping", methods=["GET", "HEAD"])
        def ping():
            return "pong", 200

        @app.route("/favicon.ico")
        def favicon():
            return "", 204

        @app.route("/health", methods=["GET", "HEAD"])
        def _h():
            with self.lock:
                snap_positions = {k: (asdict(v) if v else None) for k, v in self.positions.items() if k in self.symbols}
                rb = self.risk.balance
                dd = self.risk.drawdown
                ro = self.risk.risk_off
                pu = self.risk.paused_until
            return jsonify({
                "ok": True, "equity": rb, "dd": dd, "risk_off": ro, "paused_until": pu,
                "positions": snap_positions, "loss_bucket": self.risk.loss_bucket, "hedge_book": self.hedger.snapshot()
            }), 200

        @app.route("/metrics", methods=["GET", "HEAD"])
        def _m():
            with self.lock:
                open_positions = sum(1 for p in self.positions.values() if p)
                rb = self.risk.balance
                dd = self.risk.drawdown
                daily = self.risk.daily_pnl
            return jsonify({"equity": rb, "dd": dd, "open_positions": open_positions, "daily_pnl": daily}), 200

        threading.Thread(target=app.run, kwargs={"host": "0.0.0.0", "port": PORT}, daemon=True).start()
        self.notifier.send("🟢 Health server up. Endpoints: /, /ping, /health, /metrics")

    def _restore_state(self):
        st = self.persistence.load()
        if not st:
            return
        with self.lock:
            self.risk.balance = st.get("balance", self.risk.balance)
            self.risk.equity_peak = st.get("equity_peak", self.risk.balance)
            self.risk.daily_pnl = st.get("daily_pnl", 0.0)
            self.risk.loss_bucket = st.get("loss_bucket", 0.0)
            self.risk.cooldown = st.get("cooldown", {})
            self.risk.paused_until = st.get("paused_until", 0.0)
            setattr(self.risk, "_risk_off_until", st.get("risk_off_until", 0.0))
            for s, p in st.get("positions", {}).items():
                if p and s in self.positions:
                    try:
                        self.positions[s] = Position(**p)
                    except Exception:
                        self.positions[s] = None
            self.engine.pending_intents = st.get("pending_intents", [])
        hb = st.get("hedge_book", {})
        self.hedger.overlay_qty_target = hb.get("overlay_qty_target", 0.0)
        self.hedger.temp_qty_by_underlier = hb.get("temp_qty_by_underlier", {})
        logging.info("State restored.")

    def _sync_from_exchange(self):
        try:
            raw = self.ex.fetch_positions_live()
            live_map = {}
            for r in raw:
                market_id = r.get("symbol") or r.get("info", {}).get("symbol")
                sym = self.ex.id_to_symbol(market_id) or market_id
                if sym not in self.symbols:
                    continue
                amt = float(r.get("positionAmt") or r.get("contracts") or r.get("size") or 0)
                if abs(amt) < 1e-9:
                    continue
                side = "long" if amt > 0 else "short"
                entry = float(r.get("entryPrice") or r.get("entry_price") or 0.0)
                live_map[sym] = Position(sym, side, abs(amt), entry, 0.0, 0.0, 1.0, time.time(), mode="directional", tag="sync")
            if live_map:
                with self.lock:
                    for s in self.symbols:
                        self.positions[s] = live_map.get(s)
            logging.info(f"Live sync: {len(live_map)} positions.")
        except Exception as e:
            logging.warning(f"Live sync failed/skipped: {e}")

    def run(self):
        try:
            logging.info("Cold start: loading OHLCV...")
            self.data.cold_start()
            logging.info("Done.")
            while True:
                try:
                    self._drain_events()

                    self.engine.tick_repair(self.positions, self.risk, self.notifier, hedge_book=self.hedger.snapshot())
                    
                    self.data.schedule_updates(MAX_KLINE_UPDATES_PER_LOOP)
                    self.prices.update_next()
                    self.funding.maybe_update()
                    self.hedger.rebalance()

                    df5_map = {s: closed_bars(self.data.get(s, "5m"), "5m") for s in self.symbols}
                    df1h_map = {s: closed_bars(self.data.get(s, "1h"), "1h") for s in self.symbols}
                    btc_df5 = df5_map.get("BTC/USDT", pd.DataFrame())
                    cs_rank_map = self.strategy.cross_sectional_mom_rank(df1h_map)

                    with self.lock:
                        open_dirs = sum(1 for p in self.positions.values() if p and not p.is_hedge and p.mode != "recovery")

                    priorities: List[Tuple[float, str]] = []
                    for s in self.symbols:
                        df5 = df5_map[s]
                        df1h = df1h_map[s]
                        if df5.empty or df1h.empty or len(df1h) < 50:
                            continue
                        mom1h = float(df1h["close"].iloc[-1] / df1h["close"].iloc[-50] - 1.0)
                        closes = df5["close"]
                        n = min(80, len(closes))
                        slope = 0.0
                        if n >= 20:
                            y = closes.iloc[-n:].values
                            x = np.arange(n)
                            slope = float(np.polyfit(x, y, 1)[0] / (np.std(y) + 1e-9))
                        priorities.append((mom1h + slope, s))
                    priorities.sort(reverse=True)

                    for _, s in priorities:
                        df5 = df5_map[s]
                        df15 = closed_bars(self.data.get(s, "15m"), "15m")
                        df1h = df1h_map[s]
                        if df5.empty or df15.empty or df1h.empty:
                            continue
                        price = self.prices.get(s, df5)
                        if price is None:
                            continue
                        regime, vol_spike = self.strategy.detect_regime(df5, df15)

                        with self.lock:
                            has_pos = bool(self.positions.get(s))
                        if has_pos:
                            self._manage_position(s, price, df5)
                            continue

                        if open_dirs >= MAX_CONCURRENT_POS:
                            continue
                        if not self.risk.can_trade(s):
                            continue
                        if self.portfolio_manager.bucket_violation():
                            continue

                        if not self.risk.risk_off:
                            scores = self.strategy.alpha_scores(s, df5, df15, df1h, btc_df5, cs_rank_map.get(s, 0.0), self.funding.rates.get(s))
                            if not scores:
                                continue
                            wts = self.meta.sample_weights(s)
                            long_score = sum(wts.get(a, 0) * scores[a][0] for a in scores)
                            short_score = sum(wts.get(a, 0) * scores[a][1] for a in scores)
                            side = "long" if long_score >= short_score else "short"
                            agg_val = max(long_score, short_score)
                            thr = HV_SCORE_THRESHOLD if "High Volatility" in regime else BASE_SCORE_THRESHOLD
                            alpha_boost = self.alpha.boost(s)
                            thr *= (1.0 - min(0.2, max(0.0, alpha_boost)))
                            if agg_val < thr:
                                continue

                            f = self.strategy.features(df5, df15, df1h)
                            atr = float(f["atr"].iloc[-1]) if not np.isnan(f["atr"].iloc[-1]) else max(1e-8, float(df5["close"].diff().abs().tail(14).mean()))
                            sl_mult = 1.2 if "Ranging" in regime else 1.6
                            tp_mult = 1.6 if "Trending" in regime else 1.2
                            sl = price - atr * sl_mult if side == "long" else price + atr * sl_mult
                            tp = price + atr * tp_mult if side == "long" else price - atr * tp_mult
                            if sl <= 0 or tp <= 0 or sl == price or tp == price:
                                continue
                            lev = self.risk.dynamic_leverage(vol_spike, alpha_boost)
                            with self.lock:
                                open_list = [p for p in self.positions.values() if p]
                            base = (RISK_PER_TRADE / max(1, len(self.symbols))) * self.risk.risk_multiplier(vol_spike, alpha_boost)
                            qty = self.risk.position_size_qty(price, sl, base, open_list)
                            if qty <= 0:
                                continue
                            tag = max(wts.items(), key=lambda kv: kv[1])[0] if wts else "meta"
                            intent = (s, side, qty, price, sl, tp, lev, "directional", tag)
                            opened = self.engine.open(intent, self.positions, self.risk, self.notifier, hedge_book=self.hedger.snapshot())
                            if opened:
                                open_dirs += 1
                                self.risk.note_open(s)
                                self.hedger.open_temp_hedge_for(s, side, qty, price)

                        if RECOVERY_ENABLED and self._can_open_recovery(open_dirs):
                            f = self.strategy.features(df5, df15, df1h)
                            atr = float(f["atr"].iloc[-1])
                            slope = self.strategy.slope_dir(df5["close"])
                            px = float(df5["close"].iloc[-1])
                            mr_long = (px < f["bb_l"].iloc[-1] and f["rsi"].iloc[-1] < 32)
                            mr_short = (px > f["bb_u"].iloc[-1] and f["rsi"].iloc[-1] > 68)
                            vwap_up = (px > f["vwap"].iloc[-1] and f["macd"].iloc[-1] > 0)
                            vwap_down = (px < f["vwap"].iloc[-1] and f["macd"].iloc[-1] < 0)
                            r_side = None
                            if slope == "up" and (mr_long or vwap_up):
                                r_side = "long"
                            if slope == "down" and (mr_short or vwap_down):
                                r_side = "short"
                            if r_side and atr > 0:
                                sl_r = price - atr * RECOVERY_SCALP_SL_ATR if r_side == "long" else price + atr * RECOVERY_SCALP_SL_ATR
                                tp_r = price + atr * RECOVERY_SCALP_TP_ATR if r_side == "long" else price - atr * RECOVERY_SCALP_TP_ATR
                                budget = self.risk.recovery_risk_budget()
                                if budget > 0:
                                    qty_r = budget / max(1e-9, abs(price - sl_r))
                                    if qty_r > 0:
                                        intent = (s, r_side, qty_r, price, sl_r, tp_r, 1.0, "recovery", "recovery")
                                        opened = self.engine.open(intent, self.positions, self.risk, self.notifier, hedge_book=self.hedger.snapshot())
                                        if opened:
                                            open_dirs += 1
                                            self.risk.note_open(s)
                                            self.hedger.open_temp_hedge_for(s, r_side, qty_r, price)

                    self._hourly_report()
                    sleep_jitter(*GLOBAL_LOOP_SLEEP_RANGE)

                except HardStopTriggered:
                    prices = {s: self.prices.get(s, self.data.get(s, "5m")) for s in self.symbols}
                    self.notifier.send_critical("💥 Hard drawdown hit. Flattening safely.")
                    self.engine.flatten_all(self.positions, prices, self.risk, self.notifier, hedge_book=self.hedger.snapshot())
                    self.persistence.save(self.risk, self.positions, pending=self.engine.pending_intents, hedge_book=self.hedger.snapshot())
                    self.risk.paused_until = time.time() + 3600
                    break

                except Exception as e:
                    logging.exception(f"Loop error: {e}")
                    if time.time() - self._last_loop_error_notify > 60:
                        self.notifier.send_critical(f"⚠️ Loop error (continuing): {str(e)[:200]}")
                        self._last_loop_error_notify = time.time()
                    sleep_jitter(1.0, 2.0)

        except Exception as e:
            logging.exception("Fatal crash in run()")
            try:
                self.notifier.send_critical(f"💥 OmegaX fatal crash: {str(e)[:200]}")
            except Exception:
                pass
            raise

    def _can_open_recovery(self, open_dirs: int) -> bool:
        if self.risk.loss_bucket <= 0:
            return False
        if open_dirs >= MAX_CONCURRENT_POS:
            return False
        with self.lock:
            active = sum(1 for p in self.positions.values() if p and p.mode == "recovery")
        return active < MAX_PARALLEL_RECOVERY_TRADES

    def _pnl(self, p: Position, price: float) -> float:
        return p.dir() * p.qty * (price - p.entry)

    def _manage_position(self, s: str, price: float, df5: pd.DataFrame):
        with self.lock:
            p = self.positions.get(s)
        if not p:
            return
        if (p.side == "long" and price <= p.sl) or (p.side == "short" and price >= p.sl):
            pnl = self._pnl(p, price)
            self.engine.close(s, self.positions, price, "Stop", self.risk, self.notifier, hedge_book=self.hedger.snapshot())
            self.meta.update(s, p.tag, pnl / max(1e-9, abs(p.entry - p.sl) * max(p.qty, 1.0)))
            self.alpha.update(s, pnl)
            return
        if (p.side == "long" and price >= p.tp) or (p.side == "short" and price <= p.tp):
            pnl = self._pnl(p, price)
            self.engine.close(s, self.positions, price, "Target", self.risk, self.notifier, hedge_book=self.hedger.snapshot())
            self.meta.update(s, p.tag, pnl / max(1e-9, abs(p.entry - p.sl) * max(p.qty, 1.0)))
            self.alpha.update(s, pnl)
            return

        r = abs(p.entry - p.sl)
        atr = AverageTrueRange(df5["high"], df5["low"], df5["close"], 14).average_true_range().iloc[-1]
        changed = False

        if r > 0:
            r_mult = self._pnl(p, price) / (r * max(p.qty, 1e-9))
            if r_mult >= PARTIAL_AT_R and not p.partial:
                close_qty = p.qty * PARTIAL_FRACTION
                if self.engine.mode == Mode.PAPER:
                    pnl_part = p.dir() * close_qty * (price - p.entry)
                    with self.lock:
                        p2 = self.positions.get(s)
                        if not p2:
                            return
                        p2.qty -= close_qty
                        p2.partial = True
                        self.risk.update_on_close(pnl_part)
                    self.notifier.send(f"{s}: Partial ✔️ +${pnl_part:.2f}")
                    changed = True
                else:
                    pnl_part = self.engine.partial_close(s, close_qty, self.positions, self.risk, self.notifier, hedge_book=self.hedger.snapshot())
                    if pnl_part is not None:
                        with self.lock:
                            p2 = self.positions.get(s)
                            if not p2:
                                return
                            p2.partial = True
                        changed = True
                with self.lock:
                    p3 = self.positions.get(s)
                    if p3:
                        if p3.side == "long":
                            p3.sl = max(p3.sl, price - atr * 1.2)
                        else:
                            p3.sl = min(p3.sl, price + atr * 1.2)
                        changed = True

            trail_mult = TRAIL_ATR_BASE
            if r_mult >= 3:
                trail_mult = TRAIL_ATR_TIGHT3
            elif r_mult >= 2:
                trail_mult = TRAIL_ATR_TIGHT2
            with self.lock:
                p4 = self.positions.get(s)
                if p4:
                    if p4.side == "long" and price > p4.entry + atr:
                        new_sl = max(p4.sl, price - atr * trail_mult)
                        if new_sl > p4.sl:
                            p4.sl = new_sl
                            changed = True
                    elif p4.side == "short" and price < p4.entry - atr:
                        new_sl = min(p4.sl, price + atr * trail_mult)
                        if new_sl < p4.sl:
                            p4.sl = new_sl
                            changed = True

        with self.lock:
            p_now = self.positions.get(s)
        if p_now and p_now.pyramids < PYRAMID_MAX_STEPS and atr > 0:
            trigger = (p_now.last_add_price or p_now.entry) + (atr * PYRAMID_STEP_ATR * (1 if p_now.side == "long" else -1))
            if (p_now.side == "long" and price > trigger) or (p_now.side == "short" and price < trigger):
                add_qty = p_now.qty * PYRAMID_ADD_FRAC
                with self.lock:
                    sim = [pp for pp in self.positions.values() if pp] + [Position(p_now.symbol, p_now.side, add_qty, price, p_now.sl, p_now.tp, p_now.lev, time.time())]
                    total_risk = sum(abs(sp.entry - sp.sl) * sp.qty for sp in sim if not sp.is_hedge) / max(1e-9, self.risk.balance)
                if total_risk < MAX_TOTAL_RISK:
                    if self.engine.mode == Mode.PAPER:
                        with self.lock:
                            p_now2 = self.positions.get(s)
                            if not p_now2:
                                return
                            p_now2.entry = (p_now2.entry * p_now2.qty + price * add_qty) / (p_now2.qty + add_qty)
                            p_now2.qty += add_qty
                            p_now2.pyramids += 1
                            p_now2.last_add_price = price
                    else:
                        self.engine.add_size(s, add_qty, self.positions, self.risk, self.notifier, hedge_book=self.hedger.snapshot())
                        with self.lock:
                            p_now3 = self.positions.get(s)
                            if not p_now3:
                                return
                            p_now3.pyramids += 1
                            p_now3.last_add_price = price
                    changed = True

        if changed:
            with self.lock:
                p_final = self.positions.get(s)
            if p_final:
                self.engine.replace_brackets(s, self.positions, p_final.qty, p_final.sl, p_final.tp, self.risk, self.notifier, hedge_book=self.hedger.snapshot())

    def _hourly_report(self):
        if time.time() - self.last_report < 3600:
            return
        with self.lock:
            rb = self.risk.balance
            dd = self.risk.drawdown
            daily = self.risk.daily_pnl
        msg = f"📊 Hourly | Equity ${rb:.2f} (Δ ${rb - INITIAL_BALANCE:.2f}) | DD {dd*100:.2f}% | Daily ${daily:.2f}"
        logging.info(msg)
        self.notifier.send(msg)
        self.last_report = time.time()


if __name__ == "__main__":
    try:
        bot = OmegaXBot()
        bot.run()
    except KeyboardInterrupt:
        try:
            Notifier(TELEGRAM_ENABLED).send_critical("🛑 OmegaX stopped by KeyboardInterrupt.")
        except Exception:
            pass
        raise
    except Exception as e:
        logging.exception("Fatal crash")
        try:
            Notifier(TELEGRAM_ENABLED).send_critical(f"💥 OmegaX crashed: {str(e)[:200]}")
        except Exception:
            pass
        raise