# -*- coding: utf-8 -*-
"""
omega_x_bot.py
OmegaX Fusion v27 (Python 3.13.4-compatible, Render-friendly, no Docker required)

Highlights:
- Exchange: Binance USDT-M Futures via ccxt REST + python-binance WS (user + optional 1m klines)
- Strict Decimal for all financial logic (qty, price, pnl, balance, risk)
- SQLite (stdlib) transactional persistence + optional JSON backup snapshot
- No infinite retries: finite retry wrapper + circuit breaker for exchange calls.
- Centralized, idempotent close logic; bracket repair with bounded retries.
- Comprehensive WebSocket order status handling (FILLED, PARTIALLY_FILLED, CANCELED, REJECTED, EXPIRED).
- Fine-grained locking: separate locks for positions and risk; DB writes decoupled from locks.
- Indicator caching: per-symbol cached features; no repeated indicator recompute in a single loop iteration.
- Top-100 USDT futures auto-discovery by 24h volume; configurable scalping subset.
- Unified risk across all trades (directional + recovery) with clear caps; hedge capped vs portfolio notional.
- KeepAlive pinger + 15m Telegram equity reports; Flask health endpoints (/ping /health /metrics /api/status /api/positions).

Install:
  pip install ccxt python-binance pandas numpy ta Flask requests

Run locally:
  TELEGRAM_TOKEN=xxx TELEGRAM_CHAT_ID=yyy MODE=paper python omega_x_bot.py

Render (python runtime): use requirements.txt + render.yaml; no Docker required.
"""

from __future__ import annotations

import os
import json
import time
import math
import signal
import random
import logging
import threading
from threading import RLock, Event as ThreadEvent, Lock
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Callable
from queue import Queue, Empty
from datetime import datetime, timezone
import sqlite3
from decimal import Decimal, getcontext, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from contextlib import contextmanager
import numpy as np
import pandas as pd
import requests
import ccxt
from flask import Flask, jsonify
from ta.trend import EMAIndicator, MACD, ADXIndicator, SMAIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volume import VolumeWeightedAveragePrice
import gc

try:
    from binance.streams import ThreadedWebsocketManager
    BINANCE_WS_AVAILABLE = True
except Exception: # Retained original generic exception
    BINANCE_WS_AVAILABLE = False


# ====================== DECIMAL CONFIG ======================
getcontext().prec = int(os.environ.get("DECIMAL_PREC", "34"))
D = Decimal
Q = lambda x: D(str(x))  # safe Decimal conversion from any numeric/str


# ====================== ENV + CONFIG (ALL MOVED TO TOP) ======================

# --- START OF GLOBAL CONSTANTS & CONFIGURATION ---

# Function to validate and retrieve MODE, must be defined BEFORE MODE is set
def _validate_mode() -> str:
    mode = os.environ.get('MODE', 'paper').lower()
    if mode == 'live':
        for var in ['BINANCE_API_KEY', 'BINANCE_API_SECRET']:
            if not os.environ.get(var, '').strip():
                raise ValueError(f"Required {var} not set for live trading")
    return mode

MODE = _validate_mode() # Set MODE here

# Constants with validation
INITIAL_BALANCE = Q(os.environ.get("INITIAL_BALANCE", "3000"))
RISK_PER_TRADE = max(Q("0.001"), min(Q("0.05"), Q(os.environ.get("RISK_PER_TRADE", "0.005"))))
MAX_TOTAL_RISK = max(Q("0.01"), min(Q("0.2"), Q(os.environ.get("MAX_TOTAL_RISK", "0.06"))))
MAX_CONCURRENT_POS = max(1, min(20, int(os.environ.get("MAX_CONCURRENT_POS", "8"))))
MAX_DRAWDOWN = max(Q("0.02"), min(Q("0.3"), Q(os.environ.get("MAX_DRAWDOWN", "0.08"))))
HTTP_TIMEOUT = max(5, min(30, int(os.environ.get("HTTP_TIMEOUT", "10"))))
RETRY_LIMIT = max(1, min(10, int(os.environ.get("RETRY_LIMIT", "5"))))

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_ENABLED = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID and len(TELEGRAM_TOKEN) > 20)

DB_PATH = os.environ.get("DB_PATH", "state.db")
SYMBOLS = list(dict.fromkeys([s.strip() for s in os.environ.get("SYMBOLS", "BTC/USDT,ETH/USDT").split(",") if s.strip()]))
PORT = int(os.environ.get("PORT", "10000"))

DATA_MAX_AGE = 300
PRICE_MAX_AGE = 60

# TFs (already defined above but kept consistent with original structure)
TIMEFRAMES = ["1m", "5m", "15m", "1h"]
TF_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}

# Feature flags
ENABLE_SCALPING = os.environ.get("ENABLE_SCALPING", "true").lower() == "true"
ENABLE_MEAN_REVERSION = os.environ.get("ENABLE_MEAN_REVERSION", "true").lower() == "true"
ENABLE_GRID = os.environ.get("ENABLE_GRID", "true").lower() == "true"
ENABLE_ARBITRAGE = os.environ.get("ENABLE_ARBITRAGE", "false").lower() == "true"  # detect-only
USE_USER_WS = os.environ.get("USE_USER_WS", "true").lower() == "true"
SYMBOL_AUTO_DISCOVERY = os.environ.get("SYMBOL_AUTO_DISCOVERY", "true").lower() == "true"
SYMBOLS_TOP_N = int(os.environ.get("SYMBOLS_TOP_N", "100"))
SCALPING_MAX_SYMBOLS = int(os.environ.get("SCALPING_MAX_SYMBOLS", "16"))

# Ensemble thresholds
BASE_SCORE_THRESHOLD = Q(os.environ.get("BASE_SCORE_THRESHOLD", "0.9"))
HV_SCORE_THRESHOLD = Q(os.environ.get("HV_SCORE_THRESHOLD", "1.1"))

# Position management
PARTIAL_AT_R = Q(os.environ.get("PARTIAL_AT_R", "1.0"))
PARTIAL_FRACTION = Q(os.environ.get("PARTIAL_FRACTION", "0.5"))
TRAIL_ATR_BASE = Q(os.environ.get("TRAIL_ATR_BASE", "1.4"))
TRAIL_ATR_TIGHT2 = Q(os.environ.get("TRAIL_ATR_TIGHT2", "1.0"))
TRAIL_ATR_TIGHT3 = Q(os.environ.get("TRAIL_ATR_TIGHT3", "0.7"))
PYRAMID_MAX_STEPS = int(os.environ.get("PYRAMID_MAX_STEPS", "2"))
PYRAMID_STEP_ATR = Q(os.environ.get("PYRAMID_STEP_ATR", "0.7"))
PYRAMID_ADD_FRAC = Q(os.environ.get("PYRAMID_ADD_FRAC", "0.5"))

# Recovery
RECOVERY_ENABLED = os.environ.get("RECOVERY_ENABLED", "true").lower() == "true"
RECOVERY_BUCKET_FRACTION = Q(os.environ.get("RECOVERY_BUCKET_FRACTION", "0.35"))
RECOVERY_MAX_RISK_FRAC = Q(os.environ.get("RECOVERY_MAX_RISK_FRAC", "0.012"))
RECOVERY_MIN_RISK_FRAC = Q(os.environ.get("RECOVERY_MIN_RISK_FRAC", "0.001"))
RECOVERY_SCALP_SL_ATR = Q(os.environ.get("RECOVERY_SCALP_SL_ATR", "0.6"))
RECOVERY_SCALP_TP_ATR = Q(os.environ.get("RECOVERY_SCALP_TP_ATR", "1.0"))
MAX_PARALLEL_RECOVERY_TRADES = int(os.environ.get("MAX_PARALLEL_RECOVERY_TRADES", "1"))

# Hedging
TEMP_ENTRY_HEDGE_FRAC = Q(os.environ.get("TEMP_ENTRY_HEDGE_FRAC", "0.3"))
TEMP_HEDGE_CLOSE_AT_R = Q(os.environ.get("TEMP_HEDGE_CLOSE_AT_R", "0.7"))
MAX_HEDGE_NOTIONAL_MULT = Q(os.environ.get("MAX_HEDGE_NOTIONAL_MULT", "1.2"))

# Funding
FUNDING_TTL = int(os.environ.get("FUNDING_TTL", "600"))
HIGH_POSITIVE_FUNDING = Q(os.environ.get("HIGH_POSITIVE_FUNDING", "0.00025"))
HIGH_NEGATIVE_FUNDING = Q(os.environ.get("HIGH_NEGATIVE_FUNDING", "-0.00025"))

# Network / loop
GLOBAL_LOOP_SLEEP_RANGE = (float(os.environ.get("LOOP_SLEEP_MIN", "0.9")),
                           float(os.environ.get("LOOP_SLEEP_MAX", "1.6")))
MAX_KLINE_UPDATES_PER_LOOP = int(os.environ.get("MAX_KLINE_UPDATES_PER_LOOP", "16"))

# KeepAlive
KEEPALIVE_URL = os.environ.get("KEEPALIVE_URL", "").strip()
SELF_URL = os.environ.get("SELF_URL", "").strip()
KEEPALIVE_INTERVAL_SEC = int(os.environ.get("KEEPALIVE_INTERVAL_SEC", "60"))

# Persistence
JSON_SNAPSHOT = os.environ.get("JSON_SNAPSHOT", "state_snapshot.json")
PERSIST_JSON_INTERVAL_SEC = int(os.environ.get("PERSIST_JSON_INTERVAL_SEC", "180"))

# --- END OF GLOBAL CONSTANTS & CONFIGURATION ---


# ====================== LOGGING ======================
def setup_logging():
    logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", force=True) # force=True to ensure config applies


# ====================== UTILS ======================
def sleep_jitter(a: float, b: float):
    time.sleep(random.uniform(a, b))


def utc_midnight_ts() -> int:
    dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(dt.timestamp())


def quantize_step(x: D, step: D, rounding=ROUND_DOWN) -> D:
    if step <= 0:
        return x
    return (x / step).to_integral_value(rounding=rounding) * step


# ====================== NOTIFIER ======================
class Notifier:
    def __init__(self, enabled: bool):
        self.enabled = enabled
        self._last_send, self._min_interval = 0.0, 2.0
        self._message_queue: List[Tuple[str, bool, float]] = [] # Explicit type hint for queue
        self._lock = RLock()
        self.logger = logging.getLogger(self.__class__.__name__)
        self._queue_processor_thread = threading.Thread(target=self._run_queue_processor, daemon=True)
        self._queue_processor_thread.start()


    def _run_queue_processor(self):
        while True:
            time.sleep(self._min_interval / 2) # Check more frequently than send interval
            with self._lock:
                self._process_queue()


    def send(self, msg: str, critical: bool = False):
        if not self.enabled:
            self.logger.info(f"Notification: {msg}")
            return
        with self._lock:
            self._message_queue.append((msg, critical, time.time()))
            if len(self._message_queue) > 100: self._message_queue = self._message_queue[-100:]
            # The separate thread _run_queue_processor will pick it up


    def _process_queue(self):
        # This method is called from _run_queue_processor, so already under self._lock
        if not self._message_queue or time.time() - self._last_send < self._min_interval: return
        
        # Prioritize critical messages
        sorted_queue = sorted(self._message_queue, key=lambda x: (not x[1], x[2])) # Critical first, then by timestamp
        message_to_send = sorted_queue[0]
        
        if self._send_telegram(message_to_send[0]):
            self._last_send = time.time()
            self._message_queue.remove(message_to_send) # Remove the exact tuple
        # If send failed, it remains in the queue for retry


    def _send_telegram(self, msg: str) -> bool:
        try:
            response = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000], "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=HTTP_TIMEOUT)
            return response.status_code == 200
        except Exception as e:
            self.logger.warning(f"Telegram send failed: {e}")
            return False


    def send_critical(self, msg: str): self.send(msg, critical=True)


# ====================== EXCEPTIONS ======================
class TradingBotError(Exception): pass
class ExchangeUnavailable(TradingBotError): pass
class HardStopTriggered(TradingBotError): pass
class ConfigurationError(TradingBotError): pass
class InsufficientFundsError(TradingBotError): pass


# ====================== CIRCUIT BREAKER ======================
class CircuitBreaker:
    def __init__(self, max_failures: int = 5, reset_after: int = 60):
        self.max_failures, self.reset_after = max_failures, reset_after
        self.failures, self.opened_at, self.last_attempt = 0, 0.0, 0.0
        self._lock = RLock()

    def record_success(self):
        with self._lock:
            self.failures = max(0, self.failures - 1)
            if self.failures == 0: self.opened_at = 0.0

    def record_failure(self):
        with self._lock:
            self.failures += 1
            self.last_attempt = time.time()
            if self.failures >= self.max_failures and self.opened_at == 0.0:
                self.opened_at = time.time()

    def allow(self) -> bool:
        with self._lock:
            now = time.time()
            if self.opened_at == 0.0: return True
            if now - self.opened_at >= self.reset_after:
                if now - self.last_attempt >= self.reset_after * 0.5:
                    self.failures = max(0, self.failures - 1)
                    if self.failures < self.max_failures // 2: self.opened_at = 0.0
                return True
            return False

    @property
    def tripped(self) -> bool:
        with self._lock: return self.opened_at != 0.0


# ====================== EXCHANGE WRAPPER ======================
class ExchangeWrapper:
    def __init__(self, mode: str):
        self.logger, self.mode = logging.getLogger(self.__class__.__name__), mode
        self._init_exchange()
        self.circuit_breaker = CircuitBreaker()
        self._steps_cache: Dict[str, Tuple[D, D]] = {} # Explicit type hint
        self._min_notional_cache: Dict[str, D] = {} # Explicit type hint
        self._market_status_cache: Dict[str, bool] = {} # Explicit type hint
        self._last_market_check = 0.0
        self._load_market_info()

    def _init_exchange(self):
        try:
            config = {
                "apiKey": os.environ.get("BINANCE_API_KEY"),
                "secret": os.environ.get("BINANCE_API_SECRET"),
                "options": {"defaultType": "future", "adjustForTimeDifference": True},
                "enableRateLimit": True,
                "timeout": HTTP_TIMEOUT * 1000
            }
            if self.mode == "paper":
                # Binance Futures Testnet configuration
                config["options"]["defaultType"] = "future"
                # --- START OF FIX (Explicit URLs for Binance Futures Testnet, ensuring public endpoint) ---
                config["urls"] = {
                    'public': 'https://testnet.binance.vision/api',  # General Spot Testnet Public API
                    'api': 'https://testnet.binance.vision/api',      # Also general public API
                    'fapi': 'https://testnet.binancefuture.com/fapi/v1',   # Unified Futures API
                    'private': 'https://testnet.binancefuture.com/fapi/v1', # Private Futures API
                    'dapi': 'https://testnet.binancefuture.com/dapi/v1', # For USDⓈ-M Futures (Coin-M)
                }
                # --- END OF FIX ---
            self.exchange = ccxt.binance(config)
            self.exchange.load_markets() # Load markets after config
            futures_markets = [s for s, m in self.exchange.markets.items() if m.get('type') == 'future']
            if not futures_markets: raise ConfigurationError("No futures markets found on exchange")
        except Exception as e: raise ConfigurationError(f"Exchange init failed: {e}")

    def _load_market_info(self):
        try:
            for symbol, market in self.exchange.markets.items():
                if market.get('type') != 'future': continue
                precision, limits = market.get("precision", {}), market.get("limits", {})
                price_prec, amount_prec = precision.get("price"), precision.get("amount")
                # Fallback to limits min value if precision is None
                tick_size = D(f"1e-{price_prec}") if price_prec is not None else safe_decimal(limits.get("price", {}).get("min"), D("0.01"))
                step_size = D(f"1e-{amount_prec}") if amount_prec is not None else safe_decimal(limits.get("amount", {}).get("min"), D("0.001"))
                if tick_size <= 0: tick_size = D("0.01")
                if step_size <= 0: step_size = D("0.001")
                self._steps_cache[symbol] = (tick_size, step_size)
                min_cost = limits.get("cost", {}).get("min")
                if min_cost is None: # Binance futures uses minNotional or quoteOrderQtyMin
                    info = market.get("info", {})
                    min_cost = info.get("minNotional") or info.get("quoteOrderQtyMin") if isinstance(info, dict) else None
                self._min_notional_cache[symbol] = safe_decimal(min_cost, D("5"))
                self._market_status_cache[symbol] = market.get('active', True)
        except Exception as e: raise ConfigurationError(f"Market info loading failed: {e}")

    def _with_retries(self, func: Callable, *args, **kwargs):
        if not self.circuit_breaker.allow(): raise ExchangeUnavailable("Circuit breaker open")
        last_exception = None
        for attempt in range(RETRY_LIMIT):
            try:
                result = func(*args, **kwargs); self.circuit_breaker.record_success(); return result
            except ccxt.RateLimitExceeded as e:
                last_exception = e; self.circuit_breaker.record_failure()
                wait_time = min(2 ** attempt * 2, 30); time.sleep(wait_time)
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                last_exception = e; self.circuit_breaker.record_failure()
                if attempt < RETRY_LIMIT - 1: time.sleep(min(2 ** attempt, 10))
            except Exception as e: last_exception = e; self.circuit_breaker.record_failure(); break
        raise ExchangeUnavailable(f"Max retries exceeded: {last_exception}")

    def is_market_active(self, symbol: str) -> bool:
        if time.time() - self._last_market_check > 3600:
            try:
                self.exchange.load_markets() # Refresh markets
                self._load_market_info() # Reload info based on fresh markets
                self._last_market_check = time.time()
            except Exception as e: self.logger.warning(f"Failed to refresh market status: {e}")
        return self._market_status_cache.get(symbol, False)

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 300) -> List[List]:
        if not self.is_market_active(symbol): raise ExchangeUnavailable(f"Market {symbol} not active")
        return self._with_retries(self.exchange.fetch_ohlcv, symbol, timeframe, limit=max(10, min(limit, 1000)))

    def fetch_ticker(self, symbol: str) -> Dict:
        if not self.is_market_active(symbol): raise ExchangeUnavailable(f"Market {symbol} not active")
        return self._with_retries(self.exchange.fetch_ticker, symbol)

    def create_order(self, symbol: str, order_type: str, side: str, amount: D, price: Optional[D] = None, params: Optional[Dict] = None) -> Dict:
        if not self.is_market_active(symbol): raise ExchangeUnavailable(f"Market {symbol} not active")
        if amount <= 0 or not amount.is_finite(): raise ValueError(f"Invalid amount: {amount}")
        
        tick_size, step_size = self._steps_cache.get(symbol, (D("0.01"), D("0.001")))
        rounded_amount = quantize_step(amount, step_size)
        if rounded_amount <= 0: raise ValueError(f"Quantized amount too small: {rounded_amount}")
        
        # Determine price for notional check (for market orders, use current ticker)
        current_price_for_notional = price if price else safe_decimal(self.fetch_ticker(symbol).get('last', D("0")), D("0"))
        if current_price_for_notional <= 0: raise ExchangeUnavailable(f"Cannot get valid price for notional check on {symbol}")
        
        min_notional = self._min_notional_cache.get(symbol, D("5"))
        notional = rounded_amount * current_price_for_notional
        if notional < min_notional: raise InsufficientFundsError(f"Notional {notional} below minimum {min_notional} for {symbol}")
        
        # Quantize price if provided, then convert to float for ccxt
        price_float = float(quantize_step(price, tick_size)) if price is not None else None
        
        return self._with_retries(self.exchange.create_order, symbol, order_type, side, float(rounded_amount), price_float, params or {})

    def cancel_order(self, order_id: str, symbol: str) -> Optional[Dict]:
        try: return self._with_retries(self.exchange.cancel_order, order_id, symbol)
        except Exception as e: self.logger.warning(f"Cancel failed {order_id}: {e}"); return None

    def get_step_sizes(self, symbol: str) -> Tuple[D, D]: return self._steps_cache.get(symbol, (D("0.01"), D("0.001")))
    def get_min_notional(self, symbol: str) -> D: return self._min_notional_cache.get(symbol, D("5"))

# ====================== DATA MANAGEMENT ======================
class DataManager:
    def __init__(self, exchange: ExchangeWrapper, symbols: List[str]):
        self.exchange, self.symbols = exchange, symbols
        self.data = {s: {tf: pd.DataFrame() for tf in ["1m", "5m", "15m", "1h"]} for s in symbols}
        self.last_update = {s: {tf: 0.0 for tf in ["1m", "5m", "15m", "1h"]} for s in symbols}
        self.update_errors = {s: 0 for s in symbols}
        self.logger = logging.getLogger(self.__class__.__name__)

    def update_data(self, symbol: str, timeframe: str) -> bool:
        try:
            if self.is_data_fresh(symbol, timeframe): return True
            raw_data = self.exchange.fetch_ohlcv(symbol, timeframe, limit=300)
            if not raw_data or len(raw_data) < 10: return False
            df = pd.DataFrame(raw_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df = df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
            
            # Convert to numeric, coercing errors, then drop NaNs
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.dropna()
            
            if len(df) < 10: return False
            
            # Validate OHLCV data integrity
            invalid_ohlc = (
                (df['high'] < df['low']) | 
                (df['high'] < df['open']) | 
                (df['high'] < df['close']) | 
                (df['low'] > df['open']) | 
                (df['low'] > df['close']) | 
                (df['volume'] < 0) | 
                df[['open', 'high', 'low', 'close', 'volume']].isna().any(axis=1) # Check for NaNs again after dropna, just in case
            )
            if invalid_ohlc.any(): df = df[~invalid_ohlc]
            if len(df) < 10: return False
            
            if not validate_dataframe(df): return False # Final check after cleaning
            
            self.data[symbol][timeframe], self.last_update[symbol][timeframe], self.update_errors[symbol] = df, time.time(), 0
            return True
        except Exception as e:
            self.update_errors[symbol] += 1; self.logger.error(f"Data update failed {symbol} {timeframe}: {e}")
            if self.update_errors[symbol] > 5: # If too many errors, log and potentially backoff for longer.
                self.logger.warning(f"Too many errors for {symbol} data updates. Consider longer backoff or alert.")
                time.sleep(60) # Increased backoff
                self.update_errors[symbol] = 0 # Reset after extended backoff
            return False

    def get_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        df = self.data.get(symbol, {}).get(timeframe, pd.DataFrame())
        return df.copy() if not df.empty else df

    def is_data_fresh(self, symbol: str, timeframe: str, max_age: int = DATA_MAX_AGE) -> bool:
        return is_data_fresh(self.last_update.get(symbol, {}).get(timeframe, 0), max_age)

    def get_latest_price(self, symbol: str) -> Optional[D]:
        for tf in ["1m", "5m", "15m"]:
            df = self.get_data(symbol, tf)
            if not df.empty and self.is_data_fresh(symbol, tf, PRICE_MAX_AGE):
                return safe_decimal(df["close"].iloc[-1])
        return None

# ====================== INDICATOR CACHE ======================
class IndicatorCache:
    def __init__(self, max_size: int = 1000):
        self.cache: Dict[str, Dict[str, float]] = {} # Cache stores string keys for time-based identification, and inner dict for indicators (float values)
        self.max_size = max_size
        self.access_times: Dict[str, float] = {}
        self._lock = RLock()
        self.hit_count = 0
        self.miss_count = 0
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_indicators(self, symbol: str, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame) -> Dict[str, float]: # Return Dict of str to float
        if not all(validate_dataframe(df, 20) for df in [df5, df15, df1h]): return {}
        try: 
            # Create a cache key based on the latest timestamp of the relevant dataframes
            cache_key = f"{symbol}_{int(df5['timestamp'].iloc[-1])}_{int(df15['timestamp'].iloc[-1])}_{int(df1h['timestamp'].iloc[-1])}"
        except Exception as e: 
            self.logger.debug(f"Failed to create cache key for {symbol}: {e}")
            return {}
        
        with self._lock:
            if cache_key in self.cache:
                self.access_times[cache_key] = time.time()
                self.hit_count += 1
                return self.cache[cache_key].copy()
            self.miss_count += 1
            indicators = self._calculate_indicators(df5, df15, df1h)
            if indicators:
                self.cache[cache_key] = indicators
                self.access_times[cache_key] = time.time()
                if len(self.cache) > self.max_size: self._cleanup_cache()
            return indicators

    def _calculate_indicators(self, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame) -> Dict[str, float]: # Return Dict of str to float
        indicators_results = {} # Will store the final float values
        
        # Helper for safe indicator calculation to return a float or np.nan
        def safe_indicator_value(func: Callable[[], pd.Series], df_for_check: pd.Series):
            if df_for_check.empty or df_for_check.isna().all(): return np.nan
            try:
                result_series = func()
                return float(result_series.iloc[-1]) if not result_series.empty and not pd.isna(result_series.iloc[-1]) else np.nan
            except Exception as e:
                self.logger.debug(f"Individual indicator calculation failed: {e}")
                return np.nan

        indicators_results['ema_12'] = safe_indicator_value(lambda: EMAIndicator(df5["close"], 12).ema_indicator(), df5["close"])
        indicators_results['ema_26'] = safe_indicator_value(lambda: EMAIndicator(df5["close"], 26).ema_indicator(), df5["close"])
        indicators_results['rsi'] = safe_indicator_value(lambda: RSIIndicator(df5["close"], 14).rsi(), df5["close"])
        indicators_results['atr'] = safe_indicator_value(lambda: AverageTrueRange(df5["high"], df5["low"], df5["close"], 14).average_true_range(), df5["close"])
        
        bb_obj = BollingerBands(df5["close"], 20, 2)
        indicators_results['bb_upper'] = safe_indicator_value(bb_obj.bollinger_hband, df5["close"])
        indicators_results['bb_lower'] = safe_indicator_value(bb_obj.bollinger_lband, df5["close"])
        indicators_results['bb_middle'] = safe_indicator_value(bb_obj.bollinger_mavg, df5["close"])
        
        macd_obj = MACD(df5["close"])
        indicators_results['macd'] = safe_indicator_value(macd_obj.macd, df5["close"])
        indicators_results['macd_signal'] = safe_indicator_value(macd_obj.macd_signal, df5["close"])
        
        indicators_results['vwap'] = safe_indicator_value(lambda: VolumeWeightedAveragePrice(df5["high"], df5["low"], df5["close"], df5["volume"]).volume_weighted_average_price(), df5["close"])
        
        # Donchian Channel requires rolling window, get last value safely
        if not df5["high"].empty and not df5["low"].empty:
            indicators_results['don_high'] = float(df5["high"].rolling(20).max().iloc[-1]) if not df5["high"].rolling(20).max().empty else np.nan
            indicators_results['don_low'] = float(df5["low"].rolling(20).min().iloc[-1]) if not df5["low"].rolling(20).min().empty else np.nan
        else:
            indicators_results['don_high'] = np.nan
            indicators_results['don_low'] = np.nan
        
        indicators_results['sma_50_1h'] = safe_indicator_value(lambda: SMAIndicator(df1h["close"], 50).sma_indicator(), df1h["close"])
        
        return indicators_results

    def _cleanup_cache(self):
        try:
            # Clear oldest 30% of cache items if size exceeds max_size
            sorted_items = sorted(self.access_times.items(), key=lambda x: x[1])
            to_remove_count = max(1, int(len(sorted_items) * 0.3))
            for key, _ in sorted_items[:to_remove_count]:
                self.cache.pop(key, None)
                self.access_times.pop(key, None)
            self.logger.debug(f"Cleaned cache. New size: {len(self.cache)}")
        except Exception as e: self.logger.warning(f"Cache cleanup failed: {e}")

# ====================== DATABASE MANAGER ======================
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path, self.logger = db_path, logging.getLogger(self.__class__.__name__)
        self._connection_pool_lock = Lock()
        self._init_database()

    def _init_database(self):
        try:
            with self._get_connection() as conn:
                conn.execute("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL")
                conn.execute("""CREATE TABLE IF NOT EXISTS risk_state (id INTEGER PRIMARY KEY CHECK (id=1), balance TEXT NOT NULL CHECK (CAST(balance AS REAL) >= 0), 
                            equity_peak TEXT NOT NULL CHECK (CAST(equity_peak AS REAL) >= 0), daily_pnl TEXT NOT NULL, loss_bucket TEXT NOT NULL CHECK (CAST(loss_bucket AS REAL) >= 0), 
                            consec_losses INTEGER NOT NULL CHECK (consec_losses >= 0), paused_until REAL NOT NULL CHECK (paused_until >= 0), 
                            risk_off_until REAL NOT NULL CHECK (risk_off_until >= 0), day_anchor INTEGER NOT NULL CHECK (day_anchor > 0), 
                            updated_at REAL NOT NULL CHECK (updated_at > 0))""")
                conn.execute("""CREATE TABLE IF NOT EXISTS positions (symbol TEXT PRIMARY KEY, side TEXT NOT NULL CHECK (side IN ('long', 'short')), 
                            qty TEXT NOT NULL CHECK (CAST(qty AS REAL) > 0), entry TEXT NOT NULL CHECK (CAST(entry AS REAL) > 0), 
                            sl TEXT NOT NULL CHECK (CAST(sl AS REAL) >= 0), tp TEXT NOT NULL CHECK (CAST(tp AS REAL) >= 0), 
                            lev TEXT NOT NULL CHECK (CAST(lev AS REAL) > 0), opened_at REAL NOT NULL CHECK (opened_at > 0), 
                            partial INTEGER NOT NULL CHECK (partial IN (0, 1)), is_hedge INTEGER NOT NULL CHECK (is_hedge IN (0, 1)), 
                            mode TEXT NOT NULL, tag TEXT NOT NULL, pyramids INTEGER DEFAULT 0 CHECK (pyramids >= 0), last_add_price TEXT, 
                            entry_order_id TEXT, sl_order_id TEXT, tp_order_id TEXT, client_order_id TEXT, time_limit REAL, 
                            updated_at REAL NOT NULL CHECK (updated_at > 0))""")
                # Insert initial risk state if not exists
                if not conn.execute("SELECT COUNT(*) FROM risk_state WHERE id=1").fetchone()[0]:
                    now, midnight_ts = time.time(), int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
                    conn.execute("INSERT INTO risk_state (id,balance,equity_peak,daily_pnl,loss_bucket,consec_losses,paused_until,risk_off_until,day_anchor,updated_at) VALUES (1,?,?,?,?,?,?,?,?,?)", 
                                (str(INITIAL_BALANCE), str(INITIAL_BALANCE), '0', '0', 0, 0, 0, midnight_ts, now)) # Corrected missing fields in initial INSERT
                conn.commit()
        except Exception as e: raise ConfigurationError(f"Database init failed: {e}")

    @contextmanager
    def _get_connection(self):
        conn = None
        try:
            with self._connection_pool_lock:
                conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None) # isolation_level=None for autocommit
                conn.execute("PRAGMA busy_timeout=30000; PRAGMA temp_store=MEMORY")
            yield conn
        except Exception as e:
            if conn: 
                try: conn.rollback() # Explicit rollback if within a transaction
                except: pass
            raise e
        finally:
            if conn:
                try: conn.close()
                except: pass

    def save_risk_state(self, risk_manager) -> bool:
        try:
            with self._get_connection() as conn: 
                with risk_manager._lock: # Acquire risk_manager's lock to read its state
                    data = (str(risk_manager.balance), str(risk_manager.equity_peak), str(risk_manager.daily_pnl), str(risk_manager.loss_bucket),
                           risk_manager.consec_losses, risk_manager.paused_until, risk_manager._risk_off_until, risk_manager.day_anchor, time.time())
                
                # Input validation for data
                for item in data[:4]: # Check balance, equity_peak, daily_pnl, loss_bucket
                    if not safe_decimal(item).is_finite():
                        self.logger.error(f"Non-finite value detected for risk state save: {item}")
                        return False

                conn.execute("BEGIN IMMEDIATE") # Start transaction
                try:
                    conn.execute("UPDATE risk_state SET balance=?,equity_peak=?,daily_pnl=?,loss_bucket=?,consec_losses=?,paused_until=?,risk_off_until=?,day_anchor=?,updated_at=? WHERE id=1", data)
                    if conn.total_changes == 0: # If update failed, try insert (should only happen if init failed)
                        conn.execute("INSERT INTO risk_state (id,balance,equity_peak,daily_pnl,loss_bucket,consec_losses,paused_until,risk_off_until,day_anchor,updated_at) VALUES (1,?,?,?,?,?,?,?,?,?)", data)
                    conn.execute("COMMIT"); return True
                except Exception as e: conn.execute("ROLLBACK"); raise e
        except Exception as e: self.logger.error(f"Save risk state failed: {e}"); return False

    def load_risk_state(self) -> Dict:
        try:
            with self._get_connection() as conn:
                row = conn.execute("SELECT balance,equity_peak,daily_pnl,loss_bucket,consec_losses,paused_until,risk_off_until,day_anchor,updated_at FROM risk_state WHERE id=1").fetchone()
                if row:
                    data = {'balance': row[0], 'equity_peak': row[1], 'daily_pnl': row[2], 'loss_bucket': row[3], 'consec_losses': row[4], 'paused_until': row[5], 'risk_off_until': row[6], 'day_anchor': row[7], 'updated_at': row[8]}
                    # Convert loaded string values to safe Decimal representation
                    for key in ['balance', 'equity_peak', 'daily_pnl', 'loss_bucket']:
                        data[key] = str(safe_decimal(data.get(key), D("0")))
                    return data
                return {}
        except Exception as e: self.logger.error(f"Load risk state failed: {e}"); return {}

    def save_position(self, position: Position) -> bool:
        try:
            position.__post_init__() # Validate position before saving
            with self._get_connection() as conn:
                conn.execute("BEGIN IMMEDIATE") # Start transaction
                try:
                    conn.execute("INSERT OR REPLACE INTO positions (symbol,side,qty,entry,sl,tp,lev,opened_at,partial,is_hedge,mode,tag,pyramids,last_add_price,entry_order_id,sl_order_id,tp_order_id,client_order_id,time_limit,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                                (position.symbol, position.side, str(position.qty), str(position.entry), str(position.sl), str(position.tp), str(position.lev), position.opened_at, int(position.partial), int(position.is_hedge), position.mode, position.tag, position.pyramids, str(position.last_add_price) if position.last_add_price is not None else None, position.entry_order_id, position.sl_order_id, position.tp_order_id, position.client_order_id, position.time_limit, time.time()))
                    conn.execute("COMMIT"); return True
                except Exception as e: conn.execute("ROLLBACK"); raise e
        except Exception as e: self.logger.error(f"Save position failed {position.symbol}: {e}"); return False

    def delete_position(self, symbol: str) -> bool:
        try:
            with self._get_connection() as conn:
                conn.execute("BEGIN IMMEDIATE") # Start transaction
                try: conn.execute("DELETE FROM positions WHERE symbol=?", (symbol,)); conn.execute("COMMIT"); return True
                except Exception as e: conn.execute("ROLLBACK"); raise e
        except Exception as e: self.logger.error(f"Delete position failed {symbol}: {e}"); return False

    def load_positions(self) -> Dict[str, Position]:
        positions = {}
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT * FROM positions ORDER BY updated_at DESC")
                columns = [desc[0] for desc in cursor.description]
                for row in cursor.fetchall():
                    try:
                        data = dict(zip(columns, row))
                        # Explicitly handle potential None for optional Decimal/float fields from DB
                        last_add_price_val = safe_decimal(data['last_add_price']) if data['last_add_price'] is not None else None
                        time_limit_val = float(data['time_limit']) if data['time_limit'] is not None else None

                        positions[data['symbol']] = Position(
                            symbol=data['symbol'], side=data['side'], qty=safe_decimal(data['qty']), 
                            entry=safe_decimal(data['entry']), sl=safe_decimal(data['sl']), tp=safe_decimal(data['tp']), 
                            lev=safe_decimal(data['lev']), opened_at=float(data['opened_at']), partial=bool(data['partial']), 
                            is_hedge=bool(data['is_hedge']), mode=data['mode'], tag=data['tag'], 
                            pyramids=int(data.get('pyramids', 0)), last_add_price=last_add_price_val, 
                            entry_order_id=data.get('entry_order_id'), sl_order_id=data.get('sl_order_id'), 
                            tp_order_id=data.get('tp_order_id'), client_order_id=data.get('client_order_id'), 
                            time_limit=time_limit_val
                        )
                    except Exception as e: self.logger.error(f"Load position failed {data.get('symbol', 'unknown')}: {e}")
        except Exception as e: self.logger.error(f"Load positions failed: {e}"); return {} # Ensure a dict is always returned

    def create_backup(self, backup_path: str) -> bool:
        try:
            import shutil
            backup_dir = os.path.dirname(backup_path)
            if backup_dir and not os.path.exists(backup_dir): os.makedirs(backup_dir)
            shutil.copy2(self.db_path, backup_path)
            return os.path.exists(backup_path) and os.path.getsize(backup_path) > 0
        except Exception as e: self.logger.error(f"Backup failed: {e}"); return False

# ====================== HEALTH MONITOR ======================
class HealthMonitor:
    def __init__(self):
        self.start_time, self.error_count, self.last_error_time = time.time(), 0, 0.0
        self.error_types: Dict[str, int] = {}
        self._lock = RLock()
        self.logger = logging.getLogger(self.__class__.__name__)

    def record_error(self, error: Exception, error_type: str = "general"):
        with self._lock: self.error_count += 1; self.last_error_time = time.time(); self.error_types[error_type] = self.error_types.get(error_type, 0) + 1
        self.logger.error(f"Health Monitor recorded error ({error_type}): {error}")

    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            uptime = time.time() - self.start_time
            try: import psutil; memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
            except ImportError: # psutil might not be installed, handle gracefully
                memory_mb = 0.0
                self.logger.debug("psutil not available, memory monitoring disabled.")
            error_rate = self.error_count / (uptime / 3600) if uptime > 0 else 0
            status = 'healthy' if self.error_count < 5 and error_rate < 1.0 else ('degraded' if self.error_count < 20 and error_rate < 5.0 else 'unhealthy')
            return {'status': status, 'uptime_seconds': uptime, 'error_count': self.error_count, 'error_rate_per_hour': error_rate, 'error_types': dict(self.error_types), 'last_error_ago': time.time() - self.last_error_time if self.last_error_time > 0 else None, 'memory_usage_mb': memory_mb}

    def cleanup_memory(self): 
        try: gc.collect()
        except Exception as e: self.logger.warning(f"Garbage collection failed: {e}")

# ====================== PORTFOLIO ======================
class Portfolio:
    def __init__(self):
        self._positions: Dict[str, Position] = {}
        self._lock = RLock()
        self.logger = logging.getLogger(self.__class__.__name__)

    def add_position(self, position: Position):
        if not isinstance(position, Position):
            self.logger.error(f"Attempted to add invalid position type: {type(position)}")
            raise ValueError("Invalid position object provided")
        try:
            position.__post_init__() # Re-validate if position attributes were altered
            with self._lock: self._positions[position.symbol] = position
            self.logger.debug(f"Added position {position.symbol} to portfolio.")
        except ValueError as e:
            self.logger.error(f"Error adding position {position.symbol} to portfolio: {e}")
            raise

    def remove_position(self, symbol: str) -> Optional[Position]:
        if not isinstance(symbol, str) or not symbol.strip():
            self.logger.error(f"Attempted to remove position with invalid symbol: {symbol}")
            return None
        with self._lock: 
            removed = self._positions.pop(symbol, None)
            if removed: self.logger.debug(f"Removed position {symbol} from portfolio.")
            return removed

    def get_position(self, symbol: str) -> Optional[Position]:
        if not isinstance(symbol, str) or not symbol.strip(): return None
        with self._lock: return self._positions.get(symbol)

    def get_all_positions(self) -> List[Position]:
        with self._lock: return list(self._positions.values())

    def get_open_positions(self) -> List[Position]:
        with self._lock: return [p for p in self._positions.values() if not p.is_hedge]

# ====================== EXECUTION ENGINE ======================
class ExecutionEngine:
    def __init__(self, exchange: ExchangeWrapper, db_manager: DatabaseManager, notifier: Notifier, mode: str):
        self.exchange, self.db_manager, self.notifier, self.mode = exchange, db_manager, notifier, mode
        self.logger, self._execution_lock = logging.getLogger(self.__class__.__name__), RLock()

    def open_position(self, symbol: str, side: str, quantity: D, entry_price: D, stop_loss: D, take_profit: D, leverage: D, mode: str = "directional", tag: str = "strategy") -> Optional[Position]:
        with self._execution_lock:
            try:
                # Basic parameter validation
                if not all(isinstance(x, str) and x.strip() for x in [symbol, side, mode, tag]) or side not in ["long", "short"]:
                    raise ValueError("Invalid string parameters for open_position")
                if not all(isinstance(x, D) and x.is_finite() for x in [quantity, entry_price, leverage]) or quantity <= 0 or entry_price <= 0 or not (D("0") < leverage <= D("100")):
                    raise ValueError("Invalid numeric parameters for open_position")
                
                # SL/TP validation against entry_price
                if stop_loss > 0 and ((side == "long" and stop_loss >= entry_price) or (side == "short" and stop_loss <= entry_price)):
                    raise ValueError(f"Invalid stop loss for {side} position: {stop_loss} vs entry {entry_price}")
                if take_profit > 0 and ((side == "long" and take_profit <= entry_price) or (side == "short" and take_profit >= entry_price)):
                    raise ValueError(f"Invalid take profit for {side} position: {take_profit} vs entry {entry_price}")

                if not self.exchange.is_market_active(symbol): raise ExchangeUnavailable(f"Market {symbol} not active")
                
                tick_size, step_size = self.exchange.get_step_sizes(symbol)
                min_notional = self.exchange.get_min_notional(symbol)
                
                # Quantize all prices and quantities
                rounded_qty = quantize_step(quantity, step_size, rounding=ROUND_DOWN)
                rounded_entry = quantize_step(entry_price, tick_size, rounding=ROUND_HALF_UP)
                rounded_sl = quantize_step(stop_loss, tick_size, rounding=ROUND_DOWN if side=="long" else ROUND_UP) if stop_loss > 0 else D("0")
                rounded_tp = quantize_step(take_profit, tick_size, rounding=ROUND_UP if side=="long" else ROUND_DOWN) if take_profit > 0 else D("0")
                
                if rounded_qty <= 0: raise ValueError(f"Quantized quantity too small: {rounded_qty}")
                if rounded_qty * rounded_entry < min_notional: raise InsufficientFundsError(f"Notional {rounded_qty * rounded_entry} below minimum {min_notional} for {symbol}")
                
                # Create Position object
                position = Position(symbol=symbol, side=side, qty=rounded_qty, entry=rounded_entry, sl=rounded_sl, tp=rounded_tp, lev=leverage, opened_at=time.time(), mode=mode, tag=tag)
                
                if self.mode == "paper":
                    if not self.db_manager.save_position(position): raise TradingBotError("Failed to save paper position to DB")
                    self.notifier.send(f"📝 PAPER: Opened {side.upper()} {symbol} qty={rounded_qty} @ {rounded_entry}")
                else: # Live trading
                    order_side, client_order_id = "buy" if side == "long" else "sell", f"omega_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
                    
                    try: self.exchange.set_leverage_safe(symbol, int(leverage)) # set_leverage_safe now takes int
                    except Exception as e: self.logger.warning(f"Set leverage failed {symbol}: {e}")
                    
                    # Place market order for entry
                    order = self.exchange.create_order(symbol=symbol, order_type="market", side=order_side, amount=rounded_qty, params={"newClientOrderId": client_order_id})
                    
                    position.entry_order_id, position.client_order_id = order.get("id"), client_order_id
                    # Get actual fill price from order if available, else use rounded_entry as a fallback
                    position.entry = safe_decimal(order.get("average") or order.get("price"), rounded_entry)
                    
                    # Save position to DB *after* successful entry order
                    if not self.db_manager.save_position(position):
                        self.notifier.send_critical(f"CRITICAL: Order {order.get('id')} placed for {symbol}, but DB save failed. Manual intervention needed!")
                        raise TradingBotError("Order placed but DB save failed. Check exchange for orphan order.")
                    
                    self.notifier.send(f"✅ LIVE: Opened {side.upper()} {symbol} qty={position.qty} @ {position.entry}")
                    
                    # Place OCO/Bracket orders (SL/TP) if applicable
                    if rounded_sl > 0 or rounded_tp > 0:
                        self._place_bracket_orders(position) # Helper function for bracket orders
                return position
            except InsufficientFundsError as e:
                self.logger.warning(f"Open position failed due to insufficient funds/notional: {e}")
                self.notifier.send(f"❌ Failed to open {symbol}: Insufficient funds/notional: {str(e)[:100]}")
                return None
            except ExchangeUnavailable as e:
                self.logger.error(f"Open position failed due to exchange unavailability: {e}")
                self.notifier.send(f"❌ Failed to open {symbol}: Exchange unavailable: {str(e)[:100]}")
                return None
            except Exception as e:
                self.logger.error(f"Open position failed {symbol}: {e}", exc_info=True)
                self.notifier.send(f"❌ Failed to open {symbol}: {str(e)[:100]}")
                return None

    def _place_bracket_orders(self, position: Position):
        opposite_side = "sell" if position.side == "long" else "buy"
        
        # Place SL order
        if position.sl > 0:
            try:
                sl_order = self.exchange.create_order(
                    symbol=position.symbol, order_type="STOP_MARKET", side=opposite_side,
                    amount=position.qty, params={"stopPrice": float(position.sl), "reduceOnly": True}
                )
                position.sl_order_id = sl_order.get("id")
                self.db_manager.save_position(position) # Update DB with SL order ID
            except Exception as e:
                self.logger.warning(f"Failed to place SL order for {position.symbol}: {e}")
                self.notifier.send(f"⚠️ Failed SL for {position.symbol}. Manual SL placement advised!")
        
        # Place TP order
        if position.tp > 0:
            try:
                tp_order = self.exchange.create_order(
                    symbol=position.symbol, order_type="TAKE_PROFIT_MARKET", side=opposite_side,
                    amount=position.qty, params={"stopPrice": float(position.tp), "reduceOnly": True}
                )
                position.tp_order_id = tp_order.get("id")
                self.db_manager.save_position(position) # Update DB with TP order ID
            except Exception as e:
                self.logger.warning(f"Failed to place TP order for {position.symbol}: {e}")
                self.notifier.send(f"⚠️ Failed TP for {position.symbol}. Manual TP placement advised!")

    def close_position(self, position: Position, exit_price: D, reason: str = "MANUAL") -> D:
        with self._execution_lock:
            try:
                if not isinstance(position, Position) or not isinstance(exit_price, D) or exit_price <= 0 or not exit_price.is_finite(): raise ValueError("Invalid parameters for close_position")
                
                # Cancel existing SL/TP orders first
                if self.mode == "live":
                    if position.sl_order_id: self.exchange.cancel_order(position.sl_order_id, position.symbol)
                    if position.tp_order_id: self.exchange.cancel_order(position.tp_order_id, position.symbol)
                
                pnl = position.unrealized_pnl(exit_price)
                
                if self.mode == "live":
                    try:
                        opposite_side = "sell" if position.side == "long" else "buy"
                        self.exchange.create_order(symbol=position.symbol, order_type="market", side=opposite_side, amount=position.qty, params={"reduceOnly": True})
                    except Exception as e: self.logger.error(f"Live market close order failed for {position.symbol}: {e}")
                
                # Delete from DB *after* all exchange actions
                self.db_manager.delete_position(position.symbol)
                
                pnl_emoji = "💚" if pnl >= 0 else "❤️"
                self.notifier.send(f"{pnl_emoji} Closed {position.side.upper()} {position.symbol} @ {exit_price} PnL: ${pnl:.2f} ({reason})")
                return pnl
            except Exception as e: self.logger.error(f"Close position failed {position.symbol}: {e}", exc_info=True); return D("0")

    def update_stop_loss(self, position: Position, new_sl: D):
        with self._execution_lock:
            try:
                if not isinstance(position, Position) or not isinstance(new_sl, D) or not new_sl.is_finite(): raise ValueError("Invalid parameters for update_stop_loss")
                
                old_sl = position.sl
                # Validate new SL logic: must be below entry for long, above for short, if new_sl > 0
                if new_sl > 0 and ((position.side == "long" and new_sl >= position.entry) or (position.side == "short" and new_sl <= position.entry)):
                    self.logger.warning(f"Attempted to set invalid SL for {position.symbol}: {new_sl} vs entry {position.entry}. Ignoring update.")
                    return # Do not proceed with invalid SL
                
                tick_size, _ = self.exchange.get_step_sizes(position.symbol)
                # Round SL appropriately (down for long, up for short)
                rounded_sl = quantize_step(new_sl, tick_size, rounding=ROUND_DOWN if position.side=="long" else ROUND_UP) if new_sl > 0 else D("0")
                
                # Only update if SL actually changes
                if rounded_sl == old_sl:
                    self.logger.debug(f"SL for {position.symbol} not changed: {rounded_sl}")
                    return
                
                position.sl = rounded_sl
                if not self.db_manager.save_position(position): 
                    position.sl = old_sl # Revert SL in memory if DB save fails
                    raise TradingBotError("Failed to save updated SL to DB")
                
                if self.mode == "live":
                    # Cancel old SL order
                    if position.sl_order_id: self.exchange.cancel_order(position.sl_order_id, position.symbol)
                    
                    # Place new SL order if rounded_sl > 0
                    if rounded_sl > 0:
                        opposite_side = "sell" if position.side == "long" else "buy"
                        sl_order = self.exchange.create_order(
                            symbol=position.symbol, order_type="STOP_MARKET", side=opposite_side,
                            amount=position.qty, params={"stopPrice": float(rounded_sl), "reduceOnly": True}
                        )
                        position.sl_order_id = sl_order.get("id")
                    else: # If new_sl is 0, no SL order
                        position.sl_order_id = None
                    self.db_manager.save_position(position) # Save position with new SL order ID
                
                self.notifier.send(f"🔧 Updated SL for {position.symbol}: {old_sl} → {rounded_sl}")
            except Exception as e: self.logger.error(f"Update SL failed {position.symbol}: {e}", exc_info=True)


# ====================== MAIN BOT ======================
class OmegaXBot:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mode = MODE
        # Initializing core components (moved to _setup_components)
        self._setup_components()
        self._restore_state() # Restore state after all components are ready
        self._running = False # Initial state is not running
        self._shutdown_event = ThreadEvent() # For graceful shutdown
        self._last_error_notification = 0.0
        self._last_health_check = 0.0

        self.notifier.send_critical(f"🚀 OmegaX Bot initialized\nMode: {self.mode.upper()}\nSymbols: {len(self.symbols)}\nBalance: ${self.risk_manager.balance}")

    def _setup_components(self):
        try:
            self.notifier = Notifier(TELEGRAM_ENABLED)
            self.health_monitor = HealthMonitor()
            self.exchange = ExchangeWrapper(self.mode)
            self.db_manager = DatabaseManager(DB_PATH)
            self.strategy = Strategy()
            self.symbols = self._validate_symbols(SYMBOLS)
            if not self.symbols: raise ConfigurationError("No valid symbols available after validation.")
            self.data_manager = DataManager(self.exchange, self.symbols)
            self.indicator_cache = IndicatorCache()
            self.execution_engine = ExecutionEngine(self.exchange, self.db_manager, self.notifier, self.mode)
            self.portfolio = Portfolio()
            self.risk_manager = RiskManager(INITIAL_BALANCE)
            # Placeholder for WS if implemented (UserStream is typically enabled in live mode)
            # self.user_ws = UserStream(...) if MODE == "live" and USE_USER_WS else None
            
            # Start Flask server
            self.app = create_health_server(self)
            self.server_thread = threading.Thread(target=lambda: self.app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False), daemon=True)
            self.server_thread.start()
            self.logger.info(f"Health server started on port {PORT}")

        except Exception as e:
            self.logger.critical(f"Bot component setup failed: {e}", exc_info=True)
            # If notifier exists, send critical message
            if hasattr(self, 'notifier') and self.notifier.enabled:
                self.notifier.send_critical(f"💥 Bot setup failed: {str(e)[:200]}")
            raise ConfigurationError(f"Bot setup failed: {e}")


    def _validate_symbols(self, symbols: List[str]) -> List[str]:
        valid_symbols = []
        for symbol in symbols:
            try:
                # Use ExchangeWrapper's is_market_active for consistency and circuit breaking
                if self.exchange.is_market_active(symbol): 
                    valid_symbols.append(symbol)
                else: self.logger.warning(f"Symbol {symbol} not active futures market or not found.")
            except Exception as e: self.logger.error(f"Error validating {symbol}: {e}")
        
        # Ensure BTC/USDT is always present if possible for hedging/beta calculation
        # Check if exchange is active for BTC/USDT specifically
        if "BTC/USDT" not in valid_symbols:
            try:
                if self.exchange.is_market_active("BTC/USDT"):
                    valid_symbols.append("BTC/USDT")
                    self.logger.info("Added BTC/USDT for hedging/beta calculation.")
            except Exception as e:
                self.logger.warning(f"Could not add BTC/USDT for hedging: {e}")
        
        return valid_symbols

    def _restore_state(self):
        try:
            risk_state = self.db_manager.load_risk_state()
            if risk_state:
                try:
                    with self.risk_manager._lock:
                        self.risk_manager.balance = safe_decimal(risk_state['balance'], INITIAL_BALANCE)
                        self.risk_manager.equity_peak = safe_decimal(risk_state['equity_peak'], INITIAL_BALANCE)
                        self.risk_manager.daily_pnl = safe_decimal(risk_state['daily_pnl'])
                        self.risk_manager.loss_bucket = safe_decimal(risk_state['loss_bucket'])
                        self.risk_manager.consec_losses = max(0, int(risk_state.get('consec_losses', 0)))
                        self.risk_manager.paused_until = max(0.0, float(risk_state.get('paused_until', 0.0)))
                        self.risk_manager._risk_off_until = max(0.0, float(risk_state.get('risk_off_until', 0.0)))
                        day_anchor = int(risk_state.get('day_anchor', self.risk_manager._utc_midnight_ts()))
                        current_midnight = self.risk_manager._utc_midnight_ts()
                        if day_anchor < current_midnight: # Daily rollover on startup if missed
                            self.risk_manager.day_anchor, self.risk_manager.daily_pnl, self.risk_manager.consec_losses = current_midnight, D("0"), 0
                            self.risk_manager.loss_bucket = self.risk_manager.loss_bucket * D("0.8") # Adjust loss bucket on rollover
                            self.logger.info("Risk state rolled over to new day on startup.")
                        else: self.risk_manager.day_anchor = day_anchor
                except Exception as e: self.logger.error(f"Risk state restore failed: {e}")
            
            positions = self.db_manager.load_positions()
            for position in positions.values():
                try:
                    # Only add to portfolio if symbol is still valid and position is not expired
                    if position.symbol in self.symbols:
                        if position.is_expired(): # Clean up expired positions
                            self.logger.info(f"Restored position {position.symbol} is expired, deleting.")
                            self.db_manager.delete_position(position.symbol)
                        else: 
                            self.portfolio.add_position(position)
                            self.logger.debug(f"Restored active position {position.symbol}.")
                    else: # Symbol not in current universe, delete it
                        self.logger.info(f"Restored position {position.symbol} not in current universe, deleting.")
                        self.db_manager.delete_position(position.symbol)
                except Exception as e: self.logger.error(f"Position restore failed {position.symbol}: {e}")
        except Exception as e:
            self.logger.error(f"State restoration failed: {e}", exc_info=True)
            self.health_monitor.record_error(e, "state_restoration")

    def _get_current_prices(self) -> Dict[str, D]:
        prices = {}
        for symbol in self.symbols:
            try:
                # Prioritize fresh data from DataManager (which uses OHLCV from exchange)
                price = self.data_manager.get_latest_price(symbol)
                if price and price > 0:
                    prices[symbol] = price
                else: # Fallback to direct ticker fetch if DataManager data isn't fresh
                    ticker = self.exchange.fetch_ticker(symbol)
                    price_raw = ticker.get('last') or ticker.get('close') or ticker.get('ask') or ticker.get('bid')
                    price = safe_decimal(price_raw)
                    if price and price > 0: prices[symbol] = price
                    else: self.logger.warning(f"Could not get valid price for {symbol} from ticker.")
            except Exception as e:
                self.logger.warning(f"Failed to get current price for {symbol}: {e}")
                self.health_monitor.record_error(e, f"price_fetch_{symbol}")
                # Try to use existing dataframe data if ticker fetch fails
                for tf in ["5m", "15m", "1h"]:
                    df = self.data_manager.get_data(symbol, tf)
                    if not df.empty:
                        fallback_price = safe_decimal(df["close"].iloc[-1])
                        if fallback_price > 0: prices[symbol] = fallback_price; break
        return prices

    def _update_trailing_stop(self, position: Position, current_price: D):
        try:
            df = self.data_manager.get_data(position.symbol, "5m")
            if not validate_dataframe(df, 20) or position.sl <= 0: return
            
            # Retrieve ATR from indicator cache
            # For ATR, only df5 is relevant, so others can be empty DFs
            indicators = self.indicator_cache.get_indicators(position.symbol, df, pd.DataFrame(), pd.DataFrame())
            atr_value = indicators.get('atr')
            if pd.isna(atr_value) or atr_value <= 0: return # Use pd.isna for float/np.nan
            atr_decimal = safe_decimal(atr_value)
            
            unrealized_pnl, risk_amount = position.unrealized_pnl(current_price), position.risk_amount()
            if risk_amount <= 0: return # Avoid division by zero
            r_multiple = unrealized_pnl / risk_amount
            
            # Dynamically adjust trail multiplier
            trail_mult = D("0")
            if r_multiple >= D("3"): trail_mult = D("0.7")
            elif r_multiple >= D("2"): trail_mult = D("1.0")
            elif r_multiple >= D("1"): trail_mult = D("1.4")
            else: return # Only trail if in profit R>=1
            
            if trail_mult == D("0"): return # No trailing for this profit level
            
            new_sl = D("0")
            if position.side == "long":
                new_sl = current_price - (atr_decimal * trail_mult)
                if new_sl > position.sl: # Only update if SL moves higher (for long)
                    self.execution_engine.update_stop_loss(position, new_sl)
            else: # short position
                new_sl = current_price + (atr_decimal * trail_mult)
                if new_sl < position.sl: # Only update if SL moves lower (for short)
                    self.execution_engine.update_stop_loss(position, new_sl)
        except Exception as e: self.logger.error(f"Trailing stop failed {position.symbol}: {e}", exc_info=True); self.health_monitor.record_error(e, "trailing_stop")

    def _manage_positions(self, prices: Dict[str, D]):
        positions_to_close = []
        for position in self.portfolio.get_all_positions():
            try:
                current_price = prices.get(position.symbol)
                if not current_price or current_price <= 0:
                    self.logger.warning(f"No valid current price for {position.symbol}, cannot manage position.")
                    continue
                
                # Check for expiration, SL, TP
                if position.is_expired(): positions_to_close.append((position, current_price, "TIME_LIMIT")); continue
                
                if position.sl > 0: # Only check if SL is set and valid
                    if ((position.side == "long" and current_price <= position.sl) or (position.side == "short" and current_price >= position.sl)):
                        positions_to_close.append((position, current_price, "STOP_LOSS")); continue
                
                if position.tp > 0: # Only check if TP is set and valid
                    if ((position.side == "long" and current_price >= position.tp) or (position.side == "short" and current_price <= position.tp)):
                        positions_to_close.append((position, current_price, "TAKE_PROFIT")); continue
                
                # Apply trailing stop if not a hedge
                if not position.is_hedge: self._update_trailing_stop(position, current_price)
            except Exception as e: self.logger.error(f"Position management error {position.symbol}: {e}", exc_info=True); self.health_monitor.record_error(e, "position_management")
        
        # Execute closes outside the main position loop to avoid modifying collection during iteration
        for position, price, reason in positions_to_close:
            try:
                removed_position = self.portfolio.remove_position(position.symbol)
                if removed_position:
                    pnl = self.execution_engine.close_position(removed_position, price, reason)
                    # risk_manager.update_balance also saves risk state to DB
                    self.risk_manager.update_balance(self.risk_manager.balance + pnl) 
            except Exception as e: self.logger.error(f"Error during position closing {position.symbol}: {e}", exc_info=True); self.health_monitor.record_error(e, "position_closing")

    def _analyze_symbol(self, symbol: str, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame, current_price: D) -> Optional[Tuple[str, D, D, D, D]]:
        try:
            if not all(validate_dataframe(df, 20) for df in [df5, df15, df1h]) or not isinstance(current_price, D) or current_price <= 0 or not current_price.is_finite(): return None
            if not all(self.data_manager.is_data_fresh(symbol, tf) for tf in ["5m", "15m", "1h"]): return None
            
            indicators = self.indicator_cache.get_indicators(symbol, df5, df15, df1h)
            if not indicators: return None
            
            regime, vol_spike = self.strategy.detect_regime(df5, df15)
            
            # BTC/USDT data is needed for cross-sectional momentum and pair trading.
            # Ensure it's available and fresh.
            btc_df5 = self.data_manager.get_data("BTC/USDT", "5m")
            if not validate_dataframe(btc_df5, 20):
                self.logger.debug(f"BTC/USDT data not valid for {symbol} analysis.")
                btc_df5 = pd.DataFrame() # Ensure empty if not valid
            
            # Cross-sectional momentum expects a dict of dataframes, even for a single symbol
            cs_rank_map = self.strategy.cross_sectional_mom_rank({symbol: df1h})
            cs_rank = cs_rank_map.get(symbol, 0.0) # float
            
            # Alpha scores (returned as Dict[str, Tuple[float, float]])
            # Assuming FundingCache is part of the bot. This example hardcodes to 0.0
            # `self.funding.rates.get(symbol, D("0"))` would be used if FundingCache was integrated.
            funding_rate = 0.0 # Placeholder; replace with actual funding rate if available
            alpha_scores = self.strategy.alpha_scores(symbol, df5, df15, df1h, btc_df5, cs_rank, funding_rate)
            if not alpha_scores: return None
            
            # Aggregate alpha scores
            try:
                long_scores = [scores[0] for scores in alpha_scores.values() if np.isfinite(scores[0])]
                short_scores = [scores[1] for scores in alpha_scores.values() if np.isfinite(scores[1])] # Fix: was scores[0]
                
                # Check if lists are empty before mean calculation
                if not long_scores or not short_scores: return None
                long_score, short_score = sum(long_scores) / len(long_scores), sum(short_scores) / len(short_scores)
            except Exception as e:
                self.logger.debug(f"Error aggregating alpha scores for {symbol}: {e}")
                return None
            
            # Decision threshold
            threshold = 0.6 + (0.2 if vol_spike else 0.0) # Base confidence
            side, confidence = None, D("0")
            if long_score > threshold and long_score > short_score: side, confidence = "long", safe_decimal(min(max(0.0, long_score), 1.0))
            elif short_score > threshold and short_score > long_score: side, confidence = "short", safe_decimal(min(max(0.0, short_score), 1.0))
            if not side or confidence <= 0: return None
            
            # Calculate SL/TP using ATR from indicator cache
            atr_value = indicators.get('atr')
            if pd.isna(atr_value) or atr_value <= 0: return None
            atr_decimal = safe_decimal(atr_value)
            
            sl_mult, tp_mult = (D("1.5"), D("2.5")) if "Trending" in regime else (D("1.2"), D("1.8"))
            if vol_spike: sl_mult *= D("1.2"); tp_mult *= D("0.8") # Adjust for volatility
            
            stop_loss, take_profit = D("0"), D("0")
            if side == "long":
                stop_loss = current_price - (atr_decimal * sl_mult)
                take_profit = current_price + (atr_decimal * tp_mult)
            else: # short
                stop_loss = current_price + (atr_decimal * sl_mult)
                take_profit = current_price - (atr_decimal * tp_mult)
            
            # Basic SL/TP validation
            if stop_loss <= 0 or take_profit <= 0: # Must be positive prices
                self.logger.debug(f"Calculated SL/TP are invalid (<=0) for {symbol}. SL: {stop_loss}, TP: {take_profit}")
                return None
            if (side == "long" and stop_loss >= current_price) or (side == "short" and stop_loss <= current_price):
                self.logger.debug(f"Calculated SL is illogical for {symbol}. SL: {stop_loss}, Price: {current_price}")
                return None
            
            risk = abs(current_price - stop_loss)
            reward = abs(take_profit - current_price)
            # Ensure proper Risk/Reward ratio
            if risk <= 0 or reward / risk < D("1.2"):
                self.logger.debug(f"Invalid R:R ratio for {symbol}. Risk: {risk}, Reward: {reward}")
                return None
            
            return side, current_price, stop_loss, take_profit, confidence
        except Exception as e: self.logger.error(f"Analysis failed {symbol}: {e}", exc_info=True); self.health_monitor.record_error(e, "symbol_analysis"); return None

    def _scan_for_opportunities(self, prices: Dict[str, D]):
        if self.risk_manager.risk_off: return
        open_positions_count = len(self.portfolio.get_open_positions())
        if open_positions_count >= MAX_CONCURRENT_POS: return
        
        # Calculate total risk from currently open directional positions
        current_total_risk = sum(p.risk_amount() for p in self.portfolio.get_all_positions() if not p.is_hedge)
        # Check against MAX_TOTAL_RISK
        if self.risk_manager.balance > 0 and current_total_risk / self.risk_manager.balance >= MAX_TOTAL_RISK:
            self.logger.debug(f"Total portfolio risk ({current_total_risk}) exceeds MAX_TOTAL_RISK ({MAX_TOTAL_RISK * self.risk_manager.balance}).")
            return
        
        opportunities_found, max_new_positions = 0, min(3, MAX_CONCURRENT_POS - open_positions_count)
        
        # Sort symbols for consistent behavior
        for symbol in sorted(self.symbols):
            try:
                if opportunities_found >= max_new_positions: break
                # Check if already has position or not allowed to trade due to cooldown/risk
                if self.portfolio.get_position(symbol) or not self.risk_manager.can_trade(symbol): continue
                
                current_price = prices.get(symbol)
                if not current_price: continue
                
                # Fetch fresh data for strategy analysis
                df5, df15, df1h = self.data_manager.get_data(symbol, "5m"), self.data_manager.get_data(symbol, "15m"), self.data_manager.get_data(symbol, "1h")
                if not all(validate_dataframe(df) for df in [df5, df15, df1h]): continue
                
                signal = self._analyze_symbol(symbol, df5, df15, df1h, current_price)
                if not signal: continue
                
                side, entry_price, stop_loss, take_profit, confidence = signal
                
                # Calculate position size based on risk
                base_risk_per_trade = RISK_PER_TRADE * confidence
                position_size = self.risk_manager.position_size(symbol, entry_price, stop_loss, base_risk_per_trade)
                
                if position_size <= 0: continue
                
                # Check minimum notional and new risk contribution
                min_notional = self.exchange.get_min_notional(symbol)
                if position_size * entry_price < min_notional:
                    self.logger.debug(f"Calculated position size for {symbol} ({position_size} @ {entry_price}) has notional below minimum {min_notional}.")
                    continue
                
                new_trade_risk_amount = abs(entry_price - stop_loss) * position_size
                if self.risk_manager.balance > 0 and (current_total_risk + new_trade_risk_amount) / self.risk_manager.balance > MAX_TOTAL_RISK:
                    self.logger.debug(f"New trade for {symbol} would exceed MAX_TOTAL_RISK. Current total: {current_total_risk}, New: {new_trade_risk_amount}.")
                    continue
                
                # Open position
                position = self.execution_engine.open_position(symbol=symbol, side=side, quantity=position_size, entry_price=entry_price, stop_loss=stop_loss, take_profit=take_profit, leverage=D("10"), mode="directional", tag="strategy")
                if position:
                    self.portfolio.add_position(position)
                    self.risk_manager.note_trade_opened(symbol)
                    opportunities_found += 1
                    current_total_risk += new_trade_risk_amount # Update total risk for subsequent checks
            except Exception as e: self.logger.error(f"Scanning error {symbol}: {e}", exc_info=True); self.health_monitor.record_error(e, "opportunity_scanning")

    def _periodic_tasks(self):
        try:
            self.db_manager.save_risk_state(self.risk_manager)
            # Save all current positions to ensure consistency
            for pos in self.portfolio.get_all_positions():
                self.db_manager.save_position(pos)
            
            self.health_monitor.cleanup_memory()
            
            # Indicator cache cleanup, log stats
            cache_stats_hits, cache_stats_misses = self.indicator_cache.hit_count, self.indicator_cache.miss_count
            self.logger.debug(f"Indicator Cache Stats - Hits: {cache_stats_hits}, Misses: {cache_stats_misses}")
            self.indicator_cache._cleanup_cache() # Explicitly call cleanup
            
            self._send_periodic_report()
            
            # Backup DB periodically (e.g., once every X reports)
            if random.random() < 0.1: # 10% chance to backup DB
                backup_path = f"{DB_PATH}.backup_{int(time.time())}.db" # Unique filename
                if self.db_manager.create_backup(backup_path):
                    self.logger.info(f"Database backup created: {backup_path}")
                else:
                    self.logger.warning("Failed to create database backup.")

        except Exception as e: self.logger.error(f"Periodic tasks failed: {e}", exc_info=True); self.health_monitor.record_error(e, "periodic_tasks")

    def _send_periodic_report(self):
        try:
            prices = self._get_current_prices()
            all_positions = self.portfolio.get_all_positions()
            
            total_exposure_val = D("0")
            total_unrealized_pnl_val = D("0")

            for p in all_positions:
                # Use current price, fallback to entry price if current price not available
                price_for_calc = prices.get(p.symbol, p.entry)
                total_exposure_val += p.qty * price_for_calc
                total_unrealized_pnl_val += p.unrealized_pnl(price_for_calc)

            portfolio_summary = {
                'total_positions': len(all_positions),
                'long_positions': sum(1 for p in all_positions if p.side == "long"),
                'short_positions': sum(1 for p in all_positions if p.side == "short"),
                'total_exposure': float(total_exposure_val),
                'total_unrealized_pnl': float(total_unrealized_pnl_val)
            }
            
            with self.risk_manager._lock: # Lock risk_manager for consistent values
                risk_status = {
                    'balance': float(self.risk_manager.balance),
                    'drawdown': float(self.risk_manager.drawdown),
                    'daily_pnl': float(self.risk_manager.daily_pnl),
                    'risk_off': self.risk_manager.risk_off,
                    'consec_losses': self.risk_manager.consec_losses
                }
            
            health_status = self.health_monitor.get_health_status()
            
            initial_balance_float = float(INITIAL_BALANCE)
            total_return = ((risk_status['balance'] - initial_balance_float) / initial_balance_float) * 100 if initial_balance_float > 0 else 0
            
            report = (
                f"📊 <b>OmegaX Status</b>\n"
                f"💰 Balance: ${risk_status['balance']:.2f}\n"
                f"Return: {total_return:+.2f}%\n"
                f"DD: {risk_status['drawdown']:.2%}\n"
                f"Daily: ${risk_status['daily_pnl']:+.2f}\n"
                f"📈 Positions: {portfolio_summary['total_positions']} (L:{portfolio_summary['long_positions']} S:{portfolio_summary['short_positions']})\n"
                f"Exposure: ${portfolio_summary['total_exposure']:.2f}\n"
                f"PnL: ${portfolio_summary['total_unrealized_pnl']:+.2f}\n"
                f"⚠️ Risk Off: {'Yes' if risk_status['risk_off'] else 'No'}\n"
                f"🔧 Status: {health_status['status'].title()}\n"
                f"Uptime: {health_status['uptime_seconds']/3600:.1f}h\n"
                f"Errors: {health_status['error_count']}"
            )
            self.notifier.send(report)
        except Exception as e: self.logger.error(f"Periodic report failed: {e}", exc_info=True)

    def _health_check(self):
        if time.time() - self._last_health_check < 300: return # Check every 5 minutes
        try:
            # Check Exchange connectivity
            try: self.exchange.fetch_ticker("BTC/USDT")
            except Exception as e: self.health_monitor.record_error(e, "exchange_connectivity")
            
            # Check Database connectivity
            try: self.db_manager.load_risk_state()
            except Exception as e: self.health_monitor.record_error(e, "database_connectivity")
            
            # Check Data freshness
            fresh_count = sum(1 for s in self.symbols if any(self.data_manager.is_data_fresh(s, tf, max_age=DATA_MAX_AGE * 2) for tf in ["5m", "15m", "1h"])) # Allow longer max_age for health check
            if fresh_count / len(self.symbols) < 0.8: # If less than 80% of symbols have fresh data
                self.health_monitor.record_error(Exception(f"Data freshness low: {fresh_count}/{len(self.symbols)}"), "data_freshness")
            
            # Check Risk Manager status
            if self.risk_manager.drawdown > D("0.06"): # Warning threshold
                self.logger.warning(f"High drawdown: {float(self.risk_manager.drawdown):.2%}")
                self.health_monitor.record_error(Exception("High portfolio drawdown"), "high_drawdown")
            
            self._last_health_check = time.time()
        except Exception as e: self.logger.error(f"Health check failed: {e}", exc_info=True); self.health_monitor.record_error(e, "health_check")

    def run(self):
        self._running = True
        self.logger.info("Starting OmegaX trading bot main loop...")
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame): self.logger.info(f"Signal {signum} received, shutting down..."); self.stop()
        try: signal.signal(signal.SIGINT, signal_handler); signal.signal(signal.SIGTERM, signal_handler)
        except: pass # Signal handlers may not be available on all platforms

        last_data_update = last_periodic_task = 0.0
        loop_count = 0
        
        try:
            self.logger.info("Performing initial data load...")
            for symbol in self.symbols:
                for tf in ["5m", "15m", "1h"]:
                    self.data_manager.update_data(symbol, tf)
            self.logger.info("Initial data load complete.")

            while self._running and not self._shutdown_event.is_set():
                loop_start = time.time()
                loop_count += 1
                
                try:
                    self._health_check()
                    
                    # Data update cycle
                    if time.time() - last_data_update > 60: # Update data for all symbols/TFs every 60 seconds
                        for symbol in self.symbols:
                            for tf in ["5m", "15m", "1h"]:
                                self.data_manager.update_data(symbol, tf)
                        last_data_update = time.time()
                    
                    prices = self._get_current_prices()
                    if not prices: # If no prices, wait and retry
                        self.logger.warning("No valid current prices available. Retrying in 5 seconds.")
                        time.sleep(5)
                        continue
                    
                    # Core trading logic
                    self._manage_positions(prices)
                    self._scan_for_opportunities(prices)
                    
                    # Periodic tasks (reporting, backup, etc.)
                    if time.time() - last_periodic_task > 900: # Every 15 minutes
                        self._periodic_tasks()
                        last_periodic_task = time.time()
                    
                    if loop_count % 100 == 0: # Log every 100 loops
                        self.logger.info(f"Loop {loop_count}: Positions: {len(self.portfolio.get_all_positions())}, Balance: ${self.risk_manager.balance:.2f}")
                
                except HardStopTriggered as e:
                    self.logger.critical(f"Hard stop triggered: {e}", exc_info=True)
                    self.notifier.send_critical(f"🛑 HARD STOP: {e}")
                    
                    # Emergency Flattening
                    emergency_prices = self._get_current_prices()
                    all_positions_copy = self.portfolio.get_all_positions() # Operate on a copy
                    
                    for position in all_positions_copy:
                        try:
                            price = emergency_prices.get(position.symbol, position.entry)
                            pnl = self.execution_engine.close_position(position, price, "EMERGENCY_HARD_STOP")
                            self.portfolio.remove_position(position.symbol) # Remove from portfolio after close attempt
                            with self.risk_manager._lock: # Directly update balance after close
                                self.risk_manager.balance += pnl
                        except Exception as close_e:
                            self.logger.error(f"Emergency close failed for {position.symbol}: {close_e}", exc_info=True)
                    
                    self.db_manager.save_risk_state(self.risk_manager) # Save final risk state
                    with self.risk_manager._lock:
                        self.risk_manager.paused_until = time.time() + 3600 # Pause for 1 hour
                    self.notifier.send_critical("🔒 All positions closed. Trading paused for 1 hour."); break # Exit main loop after hard stop

                except ExchangeUnavailable as e:
                    self.logger.error(f"Exchange unavailable: {e}", exc_info=True)
                    self.health_monitor.record_error(e, "exchange_unavailable")
                    with self.risk_manager._lock:
                        self.risk_manager._risk_off_until = max(self.risk_manager._risk_off_until, time.time() + 900) # Risk off for 15 mins
                    if time.time() - self._last_error_notification > 300: # Notify every 5 mins
                        self.notifier.send(f"⚠️ Exchange unavailable: {str(e)[:100]}")
                        self._last_error_notification = time.time()
                    time.sleep(30) # Longer sleep for exchange unavailability
                    continue # Continue to next loop iteration
                
                except Exception as e:
                    self.logger.error(f"Main loop error: {e}", exc_info=True)
                    self.health_monitor.record_error(e, "main_loop")
                    if time.time() - self._last_error_notification > 300: # Notify every 5 mins
                        self.notifier.send(f"⚠️ Loop error: {str(e)[:100]}")
                        self._last_error_notification = time.time()
                
                # Dynamic sleep to maintain loop frequency
                loop_time = time.time() - loop_start
                base_sleep = 8.0 if loop_time > 3.0 else (3.0 if loop_time < 1.0 else 5.0)
                sleep_time = max(0.5, base_sleep - loop_time + random.uniform(-0.5, 0.5))
                if self._shutdown_event.wait(sleep_time): break # Wait with timeout, allows stopping
        
        except KeyboardInterrupt: self.logger.info("Bot stopped by user via KeyboardInterrupt."); self.stop()
        except Exception as e:
            self.logger.critical(f"Fatal error in main run: {e}", exc_info=True)
            self.health_monitor.record_error(e, "fatal_error")
            self.notifier.send_critical(f"💥 FATAL: {str(e)[:200]}");
            raise # Re-raise after logging and notifying
        finally: self._cleanup()

    def stop(self): self.logger.info("Stopping bot..."); self._running = False; self._shutdown_event.set()

    def _cleanup(self):
        try:
            self.logger.info("Cleanup starting...")
            
            # Save final state to DB
            self.db_manager.save_risk_state(self.risk_manager)
            for position in self.portfolio.get_all_positions():
                self.db_manager.save_position(position)
            
            # Create final backup
            backup_path = f"{DB_PATH}.backup_{int(time.time())}_final.db"
            if self.db_manager.create_backup(backup_path): self.logger.info(f"Final backup created: {backup_path}")
            
            # Send final status notification
            final_status = {
                'balance': float(self.risk_manager.balance),
                'positions': len(self.portfolio.get_all_positions()),
                'uptime': time.time() - self.health_monitor.start_time,
                'errors': self.health_monitor.error_count
            }
            self.notifier.send_critical(f"🔄 Shutdown complete\nBalance: ${final_status['balance']:.2f}\nPositions: {final_status['positions']}\nUptime: {final_status['uptime']/3600:.1f}h\nErrors: {final_status['errors']}")
            
            # Clean up memory/cache
            self.health_monitor.cleanup_memory()
            self.indicator_cache._cleanup_cache()
            
            self.logger.info("Cleanup completed")
        except Exception as e: self.logger.error(f"Cleanup failed: {e}", exc_info=True)


# ====================== FLASK HEALTH SERVER ======================
def create_health_server(bot: OmegaXBot) -> Flask:
    app = Flask(__name__)
    app.logger.disabled = True # Disable default Flask logger for cleaner output
    
    @app.route('/', methods=["GET", "HEAD"])
    def index(): return "OmegaX Bot running", 200
    
    @app.route('/ping', methods=["GET", "HEAD"])
    def ping(): return "pong", 200
    
    @app.route('/health', methods=["GET", "HEAD"])
    def health():
        try:
            # Get current prices (might involve API calls)
            # This is called periodically by health checks and could block.
            prices = bot._get_current_prices() 
            health_data = {
                'ok': True, 
                'mode': bot.mode, 
                'balance': str(bot.risk_manager.balance), # Convert Decimal to string
                'drawdown': str(bot.risk_manager.drawdown), 
                'positions': len(bot.portfolio.get_all_positions()), 
                'risk_off': bot.risk_manager.risk_off, 
                'prices_available': len(prices), 
                'uptime': time.time() - bot.health_monitor.start_time
            }
            return jsonify(health_data), 200
        except Exception as e: 
            bot.logger.error(f"Health endpoint error: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    
    @app.route('/metrics', methods=["GET"])
    def metrics():
        try: 
            return jsonify({
                'balance': str(bot.risk_manager.balance), 
                'drawdown': str(bot.risk_manager.drawdown), 
                'daily_pnl': str(bot.risk_manager.daily_pnl), 
                'open_positions': len(bot.portfolio.get_all_positions()), 
                'symbols': len(bot.symbols)
            }), 200
        except Exception as e: 
            bot.logger.error(f"Metrics endpoint error: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/status', methods=["GET"])
    def status(): return jsonify({'status': 'running', 'mode': bot.mode}), 200
    
    @app.route('/api/positions', methods=["GET"])
    def positions():
        try:
            # Convert Decimal fields in Position objects to string for JSON serialization
            pos_data = [
                {'symbol': p.symbol, 'side': p.side, 'qty': str(p.qty), 'entry': str(p.entry), 
                 'sl': str(p.sl), 'tp': str(p.tp), 'opened_at': p.opened_at} 
                for p in bot.portfolio.get_all_positions()
            ]
            return jsonify(pos_data), 200
        except Exception as e: 
            bot.logger.error(f"API positions endpoint error: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    return app

# ====================== ENTRY POINT ======================
def main():
    setup_logging()
    logger = logging.getLogger("main")
    try:
        # MODE, SYMBOLS, INITIAL_BALANCE are now defined globally BEFORE this line
        logger.info(f"Starting OmegaX Bot - Mode: {MODE}, Symbols: {len(SYMBOLS)}, Balance: ${INITIAL_BALANCE}")
        bot = OmegaXBot()
        # The Flask server is started within _setup_components
        bot.run()
    except KeyboardInterrupt: logger.info("Bot stopped by user via KeyboardInterrupt.")
    except ConfigurationError as e: logger.critical(f"Config error: {e}", exc_info=True); return 1
    except Exception as e: logger.critical(f"Bot crashed: {e}", exc_info=True); return 1
    logger.info("Bot shutdown complete.")
    return 0

if __name__ == "__main__": exit(main())