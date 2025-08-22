# -*- coding: utf-8 -*-
"""
OmegaX Trading Bot - Render-ready main.py (Paper by default)
- Core infrastructure
- Strategy and scanning
- Execution and risk
- Data management and caching
- FIXED: safe Decimal conversion in _load_market_info to avoid ConversionSyntax
"""

from __future__ import annotations

import os
import time
import random
import logging
from threading import RLock, Event as ThreadEvent
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Callable, Any
from datetime import datetime, timezone
import sqlite3
from decimal import Decimal, getcontext, ROUND_DOWN
from contextlib import contextmanager
import gc

import numpy as np
import pandas as pd
import requests

# Optional CCXT; fallback to mock if missing
try:
    import ccxt  # type: ignore
    CCXT_AVAILABLE = True
except Exception:
    CCXT_AVAILABLE = False

# TA libs (install via requirements.txt)
from ta.trend import EMAIndicator, MACD, ADXIndicator, SMAIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.momentum import RSIIndicator
from ta.volume import VolumeWeightedAveragePrice

# ====================== CONFIGURATION ======================
getcontext().prec = 34
D = Decimal

def setup_logging():
    # Console-only logging for Replit
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(level)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

def validate_config() -> str:
    required_vars = {
        'BINANCE_API_KEY': os.environ.get('BINANCE_API_KEY'),
        'BINANCE_API_SECRET': os.environ.get('BINANCE_API_SECRET')
    }
    mode = os.environ.get('MODE', 'paper').lower()
    if mode == 'live':
        for var, value in required_vars.items():
            if not value:
                raise ValueError(f"Required environment variable {var} not set for live trading")
    return mode

MODE = validate_config()

# Trading constants
SCALPING_STOP_LOSS = D(os.environ.get("SCALPING_STOP_LOSS", "0.008"))
SCALPING_PROFIT_TARGET = D(os.environ.get("SCALPING_PROFIT_TARGET", "0.012"))
SCALPING_TIME_LIMIT = int(os.environ.get("SCALPING_TIME_LIMIT", "300"))
ZSCORE_PERIOD = int(os.environ.get("ZSCORE_PERIOD", "50"))
ZSCORE_THRESHOLD = D(os.environ.get("ZSCORE_THRESHOLD", "2.0"))

# Risk parameters
INITIAL_BALANCE = D(os.environ.get("INITIAL_BALANCE", "3000"))
RISK_PER_TRADE = D(os.environ.get("RISK_PER_TRADE", "0.005"))
MAX_TOTAL_RISK = D(os.environ.get("MAX_TOTAL_RISK", "0.06"))
MAX_CONCURRENT_POS = int(os.environ.get("MAX_CONCURRENT_POS", "8"))
MAX_DRAWDOWN = D(os.environ.get("MAX_DRAWDOWN", "0.08"))

# Network settings
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "10"))
RETRY_LIMIT = int(os.environ.get("RETRY_LIMIT", "3"))

# Telegram
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_ENABLED = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)

# DB
DB_PATH = os.environ.get("DB_PATH", "state.db")

# Symbols
SYMBOLS = [s.strip() for s in os.environ.get("SYMBOLS", "BTC/USDT,ETH/USDT").split(",") if s.strip()]

# ====================== UTILITIES ======================
def safe_decimal(value: Any) -> D:
    try:
        if isinstance(value, D):
            return value
        if value is None or value == "" or value == "None":
            return D("0")
        return D(str(value))
    except Exception:
        return D("0")

def quantize_step(x: D, step: D, rounding=ROUND_DOWN) -> D:
    if step <= 0:
        return x
    return (x / step).to_integral_value(rounding=rounding) * step

# ====================== EXCEPTIONS ======================
class TradingBotError(Exception): pass
class ExchangeUnavailable(TradingBotError): pass
class HardStopTriggered(TradingBotError): pass
class ConfigurationError(TradingBotError): pass

# ====================== CIRCUIT BREAKER ======================
class CircuitBreaker:
    def __init__(self, max_failures: int = 5, reset_after: int = 60):
        self.max_failures = max_failures
        self.reset_after = reset_after
        self.failures = 0
        self.opened_at = 0.0
        self._lock = RLock()

    def record_success(self):
        with self._lock:
            self.failures = 0
            self.opened_at = 0.0

    def record_failure(self):
        with self._lock:
            self.failures += 1
            if self.failures >= self.max_failures and self.opened_at == 0.0:
                self.opened_at = time.time()

    def allow(self) -> bool:
        with self._lock:
            if self.opened_at == 0.0:
                return True
            if time.time() - self.opened_at >= self.reset_after:
                self.failures = 0
                self.opened_at = 0.0
                return True
            return False

    @property
    def tripped(self) -> bool:
        with self._lock:
            return self.opened_at != 0.0

# ====================== NOTIFIER ======================
class Notifier:
    def __init__(self, enabled: bool):
        self.enabled = enabled
        self._last_send = 0.0
        self._min_interval = 1.0
        self._lock = RLock()

    def send(self, msg: str, critical: bool = False):
        if not self.enabled:
            logging.info(f"Notification: {msg}")
            return
        with self._lock:
            now = time.time()
            if not critical and now - self._last_send < self._min_interval:
                return
            try:
                response = requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                    json={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": msg[:4000],
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True
                    },
                    timeout=HTTP_TIMEOUT
                )
                if response.status_code == 200:
                    self._last_send = now
                else:
                    logging.warning(f"Telegram error: {response.status_code} {response.text}")
            except Exception as e:
                logging.warning(f"Telegram send failed: {e}")

    def send_critical(self, msg: str):
        self.send(msg, critical=True)

# ====================== STRATEGY ======================
class Strategy:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def detect_regime(self, df5: pd.DataFrame, df15: pd.DataFrame) -> Tuple[str, bool]:
        try:
            if df5.empty or len(df5) < 50:
                return "Unknown", False
            atr = AverageTrueRange(df5["high"], df5["low"], df5["close"], 14).average_true_range()
            current_atr = float(atr.iloc[-1])
            avg_atr = float(np.nanmean(atr.tail(50)))
            vol_spike = current_atr > avg_atr * 1.5 if avg_atr > 0 else False
            adx = ADXIndicator(df5["high"], df5["low"], df5["close"], 14).adx()
            current_adx = float(adx.iloc[-1])
            if current_adx > 25:
                regime = "Trending High Volatility" if vol_spike else "Trending"
            else:
                regime = "Ranging High Volatility" if vol_spike else "Ranging"
            return regime, vol_spike
        except Exception as e:
            self.logger.warning(f"Regime detection failed: {e}")
            return "Unknown", False

    def cross_sectional_mom_rank(self, df_map: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        try:
            mom_scores = {}
            for symbol, df in df_map.items():
                if df.empty or len(df) < 50:
                    mom_scores[symbol] = 0.0
                    continue
                returns_1w = df["close"].iloc[-1] / df["close"].iloc[-7] - 1 if len(df) >= 7 else 0
                returns_1m = df["close"].iloc[-1] / df["close"].iloc[-30] - 1 if len(df) >= 30 else 0
                mom_scores[symbol] = float(returns_1w * 0.3 + returns_1m * 0.7)
            if not mom_scores:
                return {}
            sorted_scores = sorted(mom_scores.items(), key=lambda x: x[1], reverse=True)
            n = len(sorted_scores)
            ranked = {}
            for i, (symbol, _) in enumerate(sorted_scores):
                ranked[symbol] = (2 * (n - i - 1) / (n - 1) - 1) if n > 1 else 0.0
            return ranked
        except Exception as e:
            self.logger.warning(f"Cross-sectional momentum failed: {e}")
            return {}

    def alpha_scores(
        self,
        symbol: str,
        df5: pd.DataFrame,
        df15: pd.DataFrame,
        df1h: pd.DataFrame,
        btc_df5: pd.DataFrame,
        cs_rank: float,
        funding_rate: float
    ) -> Dict[str, Tuple[float, float]]:
        try:
            scores = {}
            if df5.empty or df15.empty or df1h.empty:
                return scores
            price = float(df5["close"].iloc[-1])

            # Trend following
            ema_fast = float(EMAIndicator(df5["close"], 12).ema_indicator().iloc[-1])
            ema_slow = float(EMAIndicator(df5["close"], 26).ema_indicator().iloc[-1])
            trend_long = 1.0 if price > ema_fast > ema_slow else 0.0
            trend_short = 1.0 if price < ema_fast < ema_slow else 0.0
            scores["trend"] = (trend_long, trend_short)

            # Mean reversion via Bollinger position
            bb = BollingerBands(df5["close"], 20, 2)
            bb_upper = float(bb.bollinger_hband().iloc[-1])
            bb_lower = float(bb.bollinger_lband().iloc[-1])
            denom = (bb_upper - bb_lower)
            bb_position = (price - bb_lower) / denom if denom != 0 else 0.5
            meanrev_long = max(0.0, 1 - bb_position * 2) if bb_position < 0.3 else 0.0
            meanrev_short = max(0.0, bb_position * 2 - 1) if bb_position > 0.7 else 0.0
            scores["meanrev"] = (meanrev_long, meanrev_short)

            # VWAP momentum
            vwap = float(VolumeWeightedAveragePrice(df5["high"], df5["low"], df5["close"], df5["volume"]).volume_weighted_average_price().iloc[-1])
            vwap_long = 1.0 if price > vwap * 1.001 else 0.0
            vwap_short = 1.0 if price < vwap * 0.999 else 0.0
            scores["vwap"] = (vwap_long, vwap_short)

            # Donchian breakout
            don_high = float(df5["high"].rolling(20).max().iloc[-1])
            don_low = float(df5["low"].rolling(20).min().iloc[-1])
            don_long = 1.0 if price >= don_high * 0.999 else 0.0
            don_short = 1.0 if price <= don_low * 1.001 else 0.0
            scores["donchian"] = (don_long, don_short)

            # Cross-sectional momentum
            cs_long = max(0.0, cs_rank) if cs_rank > 0.3 else 0.0
            cs_short = max(0.0, -cs_rank) if cs_rank < -0.3 else 0.0
            scores["xsmom"] = (cs_long, cs_short)

            # Funding rate carry
            carry_threshold = 0.0001
            carry_long = 1.0 if funding_rate < -carry_threshold else 0.0
            carry_short = 1.0 if funding_rate > carry_threshold else 0.0
            scores["carry"] = (carry_long, carry_short)

            # Pair trading vs BTC (aligned returns)
            if not btc_df5.empty and len(btc_df5) >= len(df5):
                asset_returns = df5["close"].pct_change().tail(20)
                btc_returns = btc_df5["close"].pct_change().reindex(asset_returns.index)
                pair_df = pd.DataFrame({"asset": asset_returns, "btc": btc_returns}).dropna()
                if len(pair_df) > 10:
                    correlation = np.corrcoef(pair_df["btc"], pair_df["asset"])[0, 1]
                    if not np.isnan(correlation) and abs(correlation) > 0.5:
                        btc_momentum = float(btc_df5["close"].iloc[-1] / btc_df5["close"].iloc[-10] - 1)
                        asset_momentum = float(df5["close"].iloc[-1] / df5["close"].iloc[-10] - 1)
                        relative_perf = asset_momentum - btc_momentum
                        pair_long = max(0.0, -relative_perf) if relative_perf < -0.02 else 0.0
                        pair_short = max(0.0, relative_perf) if relative_perf > 0.02 else 0.0
                        scores["pair"] = (pair_long, pair_short)
                    else:
                        scores["pair"] = (0.0, 0.0)
                else:
                    scores["pair"] = (0.0, 0.0)
            else:
                scores["pair"] = (0.0, 0.0)
            return scores
        except Exception as e:
            self.logger.warning(f"Alpha scores calculation failed for {symbol}: {e}")
            return {}

    def slope_dir(self, closes: pd.Series) -> str:
        try:
            if len(closes) < 20:
                return "flat"
            y = closes.tail(20).astype(float).values
            x = np.arange(len(y), dtype=float)
            slope = np.polyfit(x, y, 1)[0]
            if slope > 0:
                return "up"
            elif slope < 0:
                return "down"
            return "flat"
        except Exception:
            return "flat"

# ====================== DOMAIN MODELS ======================
@dataclass
class Position:
    symbol: str
    side: str  # "long" or "short"
    qty: D
    entry: D
    sl: D
    tp: D
    lev: D
    opened_at: float
    partial: bool = False
    is_hedge: bool = False
    mode: str = "directional"
    pyramids: int = 0
    last_add_price: Optional[D] = None
    tag: str = "meta"
    entry_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    time_limit: Optional[float] = None

    def __post_init__(self):
        if self.side not in ("long", "short"):
            raise ValueError(f"Invalid side: {self.side}")
        if self.qty <= 0:
            raise ValueError(f"Invalid quantity: {self.qty}")
        if self.entry <= 0:
            raise ValueError(f"Invalid entry price: {self.entry}")

    def dir(self) -> D:
        return D("1") if self.side == "long" else D("-1")

    def unrealized_pnl(self, current_price: D) -> D:
        return self.dir() * self.qty * (current_price - self.entry)

# ====================== RISK MANAGEMENT ======================
class RiskManager:
    def __init__(self, initial_balance: D):
        self.initial = initial_balance
        self.balance = initial_balance
        self.equity_peak = initial_balance
        self.paused_until = 0.0
        self._risk_off_until = 0.0
        self.consec_losses = 0
        self.day_anchor = self._utc_midnight_ts()
        self.daily_pnl = D("0")
        self.loss_bucket = D("0")
        self.cooldown: Dict[str, float] = {}
        self._lock = RLock()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _utc_midnight_ts(self) -> int:
        dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        return int(dt.timestamp())

    @property
    def drawdown(self) -> D:
        with self._lock:
            if self.equity_peak <= 0:
                return D("0")
            return max(D("0"), (self.equity_peak - self.balance) / self.equity_peak)

    @property
    def risk_off(self) -> bool:
        with self._lock:
            now = time.time()
            return now < self._risk_off_until or now < self.paused_until

    def update_balance(self, new_balance: D):
        with self._lock:
            old_balance = self.balance
            self.balance = new_balance
            self.equity_peak = max(self.equity_peak, self.balance)
            pnl_change = new_balance - old_balance
            if time.time() >= self.day_anchor + 86400:
                self.day_anchor = self._utc_midnight_ts()
                self.daily_pnl = D("0")
                self.consec_losses = 0
                self.loss_bucket = D("0")
            self.daily_pnl += pnl_change
            if pnl_change < 0:
                self.consec_losses += 1
                self.loss_bucket += abs(pnl_change)
            else:
                self.consec_losses = 0
                if self.loss_bucket > 0:
                    self.loss_bucket = max(D("0"), self.loss_bucket - pnl_change * D("0.5"))
        self._check_risk_limits()

    def _check_risk_limits(self):
        dd = self.drawdown
        if dd >= MAX_DRAWDOWN:
            raise HardStopTriggered(f"Drawdown {float(dd):.2%} exceeds limit {float(MAX_DRAWDOWN):.2%}")
        if dd >= D("0.05"):
            with self._lock:
                self.paused_until = max(self.paused_until, time.time() + 1800)
            self.logger.warning(f"Soft pause activated: drawdown {float(dd):.2%}")

    def can_trade(self, symbol: str) -> bool:
        with self._lock:
            if self.risk_off:
                return False
            return time.time() >= self.cooldown.get(symbol, 0.0)

    def note_trade_opened(self, symbol: str):
        with self._lock:
            self.cooldown[symbol] = time.time() + 300  # 5 min cooldown

    def position_size(self, symbol: str, entry: D, sl: D, risk_fraction: D) -> D:
        if entry <= 0 or sl <= 0 or entry == sl:
            return D("0")
        with self._lock:
            risk_amount = self.balance * risk_fraction
            price_risk = abs(entry - sl)
            if price_risk <= 0:
                return D("0")
            return risk_amount / price_risk

# ====================== MOCK EXCHANGE (fallback) ======================
class MockExchange:
    """
    Offline mock exchange that generates deterministic random-walk OHLCV and basic ticker/order stubs.
    Helps the bot run on Replit without external network.
    """
    def __init__(self, symbols: List[str]):
        self._symbols = list(dict.fromkeys(symbols + (["BTC/USDT"] if "BTC/USDT" not in symbols else [])))
        self._last_prices: Dict[str, float] = {}
        self.markets = {}
        for s in self._symbols:
            self.markets[s] = {
                "precision": {"price": 2, "amount": 6},
                "limits": {"cost": {"min": 5.0}, "price": {"min": 0.01}, "amount": {"min": 0.000001}},
                "info": {}
            }

    def _base_price(self, symbol: str) -> float:
        if "BTC" in symbol:
            return 60000.0
        if "ETH" in symbol:
            return 3000.0
        return 100.0

    def _tf_seconds(self, timeframe: str) -> int:
        return {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}.get(timeframe, 60)

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 300) -> List[List]:
        sec = self._tf_seconds(timeframe)
        now_ms = int(time.time()) * 1000
        start_ms = now_ms - limit * sec * 1000
        price = self._last_prices.get(symbol, self._base_price(symbol))
        out = []
        rng = random.Random(hash(symbol + timeframe) % (2**32))
        for i in range(limit):
            ts = start_ms + i * sec * 1000
            drift = 0.0002
            shock = rng.uniform(-0.002, 0.002)
            open_ = price
            close = max(0.01, open_ * (1 + drift + shock))
            high = max(open_, close) * (1 + abs(rng.uniform(0, 0.001)))
            low = min(open_, close) * (1 - abs(rng.uniform(0, 0.001)))
            vol = abs(rng.gauss(100, 30))
            out.append([ts, open_, high, low, close, vol])
            price = close
        self._last_prices[symbol] = price
        return out

    def fetch_ticker(self, symbol: str) -> Dict:
        price = self._last_prices.get(symbol)
        if price is None:
            self.fetch_ohlcv(symbol, "5m", limit=5)
            price = self._last_prices.get(symbol, self._base_price(symbol))
        return {"symbol": symbol, "last": price}

    def create_order(self, symbol: str, order_type: str, side: str, amount: float, price: Optional[float], params: Dict) -> Dict:
        return {
            "id": f"mock-{int(time.time()*1000)}",
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
            "status": "filled"
        }

    def cancel_order(self, order_id: str, symbol: str) -> Dict:
        return {"id": order_id, "symbol": symbol, "status": "canceled"}

    def fetch_balance(self) -> Dict:
        return {"free": {"USDT": 10000}, "total": {"USDT": 10000}}

# ====================== EXCHANGE WRAPPER ======================
class ExchangeWrapper:
    def __init__(self, mode: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mode = mode
        self._steps_cache: Dict[str, Tuple[D, D]] = {}
        self._min_notional_cache: Dict[str, D] = {}
        self.circuit_breaker = CircuitBreaker()
        self.offline = False
        self._init_exchange()
        self._load_market_info()

    def _init_exchange(self):
        symbols = list(dict.fromkeys(SYMBOLS + (["BTC/USDT"] if "BTC/USDT" not in SYMBOLS else [])))
        if not CCXT_AVAILABLE:
            self.logger.warning("ccxt not available, using MockExchange.")
            self.exchange = MockExchange(symbols)
            self.offline = True
            return
        try:
            self.exchange = ccxt.binance({
                "apiKey": os.environ.get("BINANCE_API_KEY"),
                "secret": os.environ.get("BINANCE_API_SECRET"),
                "options": {"defaultType": "future", "adjustForTimeDifference": True},
                "enableRateLimit": True,
                "timeout": HTTP_TIMEOUT * 1000
            })
            self.exchange.load_markets()
            self.logger.info("Exchange initialized")
        except Exception as e:
            self.logger.warning(f"Falling back to MockExchange due to error: {e}")
            self.exchange = MockExchange(symbols)
            self.offline = True

    def _load_market_info(self):
        """
        FIX: Safely compute Decimal tick/step sizes even if precision fields are missing/invalid.
        """
        for symbol, market in self.exchange.markets.items():
            precision = market.get("precision", {})
            limits = market.get("limits", {})
            price_prec = precision.get("price")
            amount_prec = precision.get("amount")

            # --- Price tick size ---
            tick_size = None
            if isinstance(price_prec, int) and price_prec >= 0:
                tick_size = D(f"1e-{price_prec}")
            elif limits.get("price", {}).get("min") is not None:
                tick_size = safe_decimal(limits["price"]["min"])
            if not tick_size or tick_size <= 0:
                tick_size = D("0.01")  # safe fallback

            # --- Amount step size ---
            step_size = None
            if isinstance(amount_prec, int) and amount_prec >= 0:
                step_size = D(f"1e-{amount_prec}")
            elif limits.get("amount", {}).get("min") is not None:
                step_size = safe_decimal(limits["amount"]["min"])
            if not step_size or step_size <= 0:
                step_size = D("0.000001")  # conservative fallback

            self._steps_cache[symbol] = (tick_size, step_size)

            # --- Minimum notional (min cost/value per order) ---
            min_cost = limits.get("cost", {}).get("min")
            if not min_cost:
                info = market.get("info", {})
                min_cost = info.get("minNotional") if isinstance(info, dict) else None
            self._min_notional_cache[symbol] = safe_decimal(min_cost) if min_cost else D("5")

    def _with_retries(self, func: Callable, *args, **kwargs):
        if not self.circuit_breaker.allow():
            raise ExchangeUnavailable("Circuit breaker is open")
        last_exception = None
        for attempt in range(RETRY_LIMIT):
            try:
                result = func(*args, **kwargs)
                self.circuit_breaker.record_success()
                return result
            except Exception as e:
                last_exception = e
                self.circuit_breaker.record_failure()
                self.logger.warning(f"Exchange error (attempt {attempt + 1}): {e}")
                if attempt < RETRY_LIMIT - 1:
                    time.sleep(min(2 ** attempt, 10))
        raise ExchangeUnavailable(f"Max retries exceeded: {last_exception}")

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 300) -> List[List]:
        return self._with_retries(self.exchange.fetch_ohlcv, symbol, timeframe, limit=limit)

    def fetch_ticker(self, symbol: str) -> Dict:
        return self._with_retries(self.exchange.fetch_ticker, symbol)

    def create_order(self, symbol: str, order_type: str, side: str, 
                    amount: D, price: Optional[D] = None, params: Optional[Dict] = None) -> Dict:
        if amount <= 0:
            raise ValueError(f"Invalid amount: {amount}")
        tick_size, step_size = self._steps_cache.get(symbol, (D("0.01"), D("0.000001")))
        rounded_amount = quantize_step(amount, step_size)
        if rounded_amount <= 0:
            raise ValueError(f"Amount too small after rounding: {amount} -> {rounded_amount}")
        order_params = params or {}
        price_float = float(quantize_step(price, tick_size)) if price is not None else None
        return self._with_retries(
            self.exchange.create_order,
            symbol, order_type, side, float(rounded_amount), price_float, order_params
        )

    def cancel_order(self, order_id: str, symbol: str) -> Optional[Dict]:
        try:
            return self._with_retries(self.exchange.cancel_order, order_id, symbol)
        except Exception as e:
            self.logger.warning(f"Failed to cancel order {order_id}: {e}")
            return None

    def fetch_balance(self) -> Dict:
        return self._with_retries(self.exchange.fetch_balance)

    def get_step_sizes(self, symbol: str) -> Tuple[D, D]:
        return self._steps_cache.get(symbol, (D("0.01"), D("0.000001")))

    def get_min_notional(self, symbol: str) -> D:
        return self._min_notional_cache.get(symbol, D("5"))

# ====================== DATA MANAGEMENT ======================
class DataManager:
    def __init__(self, exchange: ExchangeWrapper, symbols: List[str]):
        self.exchange = exchange
        self.symbols = symbols
        self.data: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.last_update: Dict[str, Dict[str, float]] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        timeframes = ["1m", "5m", "15m", "1h"]
        for symbol in symbols:
            self.data[symbol] = {tf: pd.DataFrame() for tf in timeframes}
            self.last_update[symbol] = {tf: 0.0 for tf in timeframes}

    def update_data(self, symbol: str, timeframe: str) -> bool:
        try:
            raw_data = self.exchange.fetch_ohlcv(symbol, timeframe, limit=300)
            if not raw_data:
                return False
            df = pd.DataFrame(raw_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df = df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
            if df.empty or len(df) < 10:
                self.logger.warning(f"Insufficient data for {symbol} {timeframe}")
                return False
            numeric_cols = ["open", "high", "low", "close", "volume"]
            df[numeric_cols] = df[numeric_cols].astype(float)
            self.data[symbol][timeframe] = df
            self.last_update[symbol][timeframe] = time.time()
            return True
        except Exception as e:
            self.logger.error(f"Failed to update data for {symbol} {timeframe}: {e}")
            return False

    def get_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        return self.data.get(symbol, {}).get(timeframe, pd.DataFrame())

    def is_data_fresh(self, symbol: str, timeframe: str, max_age: int = 300) -> bool:
        last_update = self.last_update.get(symbol, {}).get(timeframe, 0)
        return time.time() - last_update < max_age

# ====================== INDICATOR CACHE ======================
class IndicatorCache:
    def __init__(self, max_size: int = 1000):
        self.cache: Dict[str, Dict] = {}
        self.max_size = max_size
        self.access_times: Dict[str, float] = {}
        self._lock = RLock()

    def get_indicators(self, symbol: str, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame) -> Dict:
        if df5.empty or df15.empty or df1h.empty:
            return {}
        cache_key = f"{symbol}_{int(df5['timestamp'].iloc[-1])}"
        with self._lock:
            if cache_key in self.cache:
                self.access_times[cache_key] = time.time()
                return self.cache[cache_key]
            indicators = self._calculate_indicators(df5, df15, df1h)
            self.cache[cache_key] = indicators
            self.access_times[cache_key] = time.time()
            if len(self.cache) > self.max_size:
                self._cleanup_cache()
            return indicators

    def _calculate_indicators(self, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame) -> Dict:
        try:
            indicators = {}
            indicators['ema_12'] = EMAIndicator(df5["close"], 12).ema_indicator()
            indicators['ema_26'] = EMAIndicator(df5["close"], 26).ema_indicator()
            indicators['rsi'] = RSIIndicator(df5["close"], 14).rsi()
            indicators['atr'] = AverageTrueRange(df5["high"], df5["low"], df5["close"], 14).average_true_range()
            bb = BollingerBands(df5["close"], 20, 2)
            indicators['bb_upper'] = bb.bollinger_hband()
            indicators['bb_lower'] = bb.bollinger_lband()
            indicators['bb_middle'] = bb.bollinger_mavg()
            macd = MACD(df5["close"])
            indicators['macd'] = macd.macd()
            indicators['macd_signal'] = macd.macd_signal()
            indicators['vwap'] = VolumeWeightedAveragePrice(
                df5["high"], df5["low"], df5["close"], df5["volume"]
            ).volume_weighted_average_price()
            indicators['don_high'] = df5["high"].rolling(20).max()
            indicators['don_low'] = df5["low"].rolling(20).min()
            if len(df1h) >= 50:
                indicators['sma_50_1h'] = SMAIndicator(df1h["close"], 50).sma_indicator()
            else:
                indicators['sma_50_1h'] = pd.Series([np.nan] * len(df1h), index=df1h.index)
            return indicators
        except Exception as e:
            logging.error(f"Indicator calculation failed: {e}")
            return {}

    def _cleanup_cache(self):
        sorted_items = sorted(self.access_times.items(), key=lambda x: x[1])
        to_remove = int(len(sorted_items) * 0.2)
        for key, _ in sorted_items[:to_remove]:
            self.cache.pop(key, None)
            self.access_times.pop(key, None)

# ====================== DATABASE MANAGER ======================
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.logger = logging.getLogger(self.__class__.__name__)
        self._init_database()

    def _init_database(self):
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS risk_state (
                        id INTEGER PRIMARY KEY CHECK (id=1),
                        balance TEXT NOT NULL,
                        equity_peak TEXT NOT NULL,
                        daily_pnl TEXT NOT NULL,
                        loss_bucket TEXT NOT NULL,
                        consec_losses INTEGER NOT NULL,
                        paused_until REAL NOT NULL,
                        risk_off_until REAL NOT NULL,
                        day_anchor INTEGER NOT NULL,
                        updated_at REAL NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS positions (
                        symbol TEXT PRIMARY KEY,
                        side TEXT NOT NULL,
                        qty TEXT NOT NULL,
                        entry TEXT NOT NULL,
                        sl TEXT NOT NULL,
                        tp TEXT NOT NULL,
                        lev TEXT NOT NULL,
                        opened_at REAL NOT NULL,
                        partial INTEGER NOT NULL,
                        is_hedge INTEGER NOT NULL,
                        mode TEXT NOT NULL,
                        tag TEXT NOT NULL,
                        pyramids INTEGER DEFAULT 0,
                        last_add_price TEXT,
                        entry_order_id TEXT,
                        sl_order_id TEXT,
                        tp_order_id TEXT,
                        client_order_id TEXT,
                        time_limit REAL,
                        updated_at REAL NOT NULL
                    )
                """)
                cursor = conn.execute("SELECT COUNT(*) FROM risk_state WHERE id=1")
                row = cursor.fetchone()
                count = row[0] if row else 0
                if count == 0:
                    conn.execute("""
                        INSERT INTO risk_state 
                        (id, balance, equity_peak, daily_pnl, loss_bucket, consec_losses, 
                         paused_until, risk_off_until, day_anchor, updated_at)
                        VALUES (1, ?, ?, '0', '0', 0, 0, 0, ?, ?)
                    """, (str(INITIAL_BALANCE), str(INITIAL_BALANCE), int(time.time()), time.time()))
                conn.commit()
        except Exception as e:
            raise ConfigurationError(f"Database initialization failed: {e}")

    @contextmanager
    def _get_connection(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()

    def save_risk_state(self, risk_manager: RiskManager):
        try:
            with self._get_connection() as conn:
                with risk_manager._lock:
                    conn.execute("""
                        UPDATE risk_state SET
                            balance = ?, equity_peak = ?, daily_pnl = ?, loss_bucket = ?,
                            consec_losses = ?, paused_until = ?, risk_off_until = ?,
                            day_anchor = ?, updated_at = ?
                        WHERE id = 1
                    """, (
                        str(risk_manager.balance),
                        str(risk_manager.equity_peak),
                        str(risk_manager.daily_pnl),
                        str(risk_manager.loss_bucket),
                        risk_manager.consec_losses,
                        risk_manager.paused_until,
                        risk_manager._risk_off_until,
                        risk_manager.day_anchor,
                        time.time()
                    ))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to save risk state: {e}")

    def load_risk_state(self) -> Dict:
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("""
                    SELECT balance, equity_peak, daily_pnl, loss_bucket, consec_losses,
                           paused_until, risk_off_until, day_anchor
                    FROM risk_state WHERE id = 1
                """)
                row = cursor.fetchone()
                if row:
                    return {
                        'balance': row[0],
                        'equity_peak': row[1],
                        'daily_pnl': row[2],
                        'loss_bucket': row[3],
                        'consec_losses': row[4],
                        'paused_until': row[5],
                        'risk_off_until': row[6],
                        'day_anchor': row[7]
                    }
                else:
                    return {}
        except Exception as e:
            self.logger.error(f"Failed to load risk state: {e}")
            return {}

    def save_position(self, position: Position):
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO positions 
                    (symbol, side, qty, entry, sl, tp, lev, opened_at, partial, is_hedge,
                     mode, tag, pyramids, last_add_price, entry_order_id, sl_order_id,
                     tp_order_id, client_order_id, time_limit, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    position.symbol, position.side, str(position.qty), str(position.entry),
                    str(position.sl), str(position.tp), str(position.lev), position.opened_at,
                    int(position.partial), int(position.is_hedge), position.mode, position.tag,
                    position.pyramids, str(position.last_add_price) if position.last_add_price else None,
                    position.entry_order_id, position.sl_order_id, position.tp_order_id,
                    position.client_order_id, position.time_limit, time.time()
                ))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to save position {position.symbol}: {e}")

    def delete_position(self, symbol: str):
        try:
            with self._get_connection() as conn:
                conn.execute("DELETE FROM positions WHERE symbol = ?", (symbol,))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to delete position {symbol}: {e}")

    def load_positions(self) -> Dict[str, Position]:
        positions = {}
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT * FROM positions")
                columns = [desc[0] for desc in cursor.description]
                for row in cursor.fetchall():
                    data = dict(zip(columns, row))
                    position = Position(
                        symbol=data['symbol'],
                        side=data['side'],
                        qty=safe_decimal(data['qty']),
                        entry=safe_decimal(data['entry']),
                        sl=safe_decimal(data['sl']),
                        tp=safe_decimal(data['tp']),
                        lev=safe_decimal(data['lev']),
                        opened_at=data['opened_at'],
                        partial=bool(data['partial']),
                        is_hedge=bool(data['is_hedge']),
                        mode=data['mode'],
                        tag=data['tag'],
                        pyramids=data.get('pyramids', 0),
                        last_add_price=safe_decimal(data['last_add_price']) if data['last_add_price'] else None,
                        entry_order_id=data.get('entry_order_id'),
                        sl_order_id=data.get('sl_order_id'),
                        tp_order_id=data.get('tp_order_id'),
                        client_order_id=data.get('client_order_id'),
                        time_limit=data.get('time_limit')
                    )
                    positions[position.symbol] = position
        except Exception as e:
            self.logger.error(f"Failed to load positions: {e}")
        return positions

    def create_backup(self, backup_path: str):
        try:
            import shutil
            shutil.copy2(self.db_path, backup_path)
            self.logger.info(f"Database backup created: {backup_path}")
        except Exception as e:
            self.logger.error(f"Failed to create backup: {e}")

# ====================== HEALTH MONITOR ======================
class HealthMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.error_count = 0
        self.last_error_time = 0.0
        self._lock = RLock()

    def record_error(self, error: Exception):
        with self._lock:
            self.error_count += 1
            self.last_error_time = time.time()

    def get_health_status(self) -> Dict:
        with self._lock:
            uptime = time.time() - self.start_time
            try:
                import psutil
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
            except Exception:
                memory_mb = 0.0
            return {
                'uptime_seconds': uptime,
                'error_count': self.error_count,
                'last_error_ago': time.time() - self.last_error_time if self.last_error_time > 0 else None,
                'memory_usage_mb': memory_mb,
                'status': 'healthy' if self.error_count < 10 else 'degraded'
            }

    def cleanup_memory(self):
        gc.collect()

# ====================== PORTFOLIO ======================
class Portfolio:
    def __init__(self):
        self._positions: Dict[str, Position] = {}
        self._lock = RLock()

    def add_position(self, position: Position):
        with self._lock:
            self._positions[position.symbol] = position

    def remove_position(self, symbol: str):
        with self._lock:
            if symbol in self._positions:
                self._positions.pop(symbol, None)

    def get_position(self, symbol: str) -> Optional[Position]:
        with self._lock:
            return self._positions.get(symbol)

    def get_all_positions(self) -> List[Position]:
        with self._lock:
            return list(self._positions.values())

    def get_open_positions(self) -> List[Position]:
        return self.get_all_positions()

# ====================== EXECUTION ENGINE ======================
class ExecutionEngine:
    def __init__(self, exchange: ExchangeWrapper, db_manager: DatabaseManager, notifier: Notifier, mode: str):
        self.exchange = exchange
        self.db_manager = db_manager
        self.notifier = notifier
        self.mode = mode
        self.logger = logging.getLogger(self.__class__.__name__)
        self._lock = RLock()

    def open_position(
        self,
        symbol: str,
        side: str,
        quantity: D,
        entry_price: D,
        stop_loss: D,
        take_profit: D,
        leverage: D,
        mode: str = "directional",
        tag: str = "strategy"
    ) -> Optional[Position]:
        try:
            if quantity <= 0:
                raise ValueError(f"Invalid quantity: {quantity}")
            if entry_price <= 0:
                raise ValueError(f"Invalid entry price: {entry_price}")

            tick_size, step_size = self.exchange.get_step_sizes(symbol)
            rounded_qty = quantize_step(quantity, step_size)
            rounded_entry = quantize_step(entry_price, tick_size)
            rounded_sl = quantize_step(stop_loss, tick_size) if stop_loss > 0 else D("0")
            rounded_tp = quantize_step(take_profit, tick_size) if take_profit > 0 else D("0")

            if rounded_qty <= 0:
                self.logger.warning(f"Quantity too small after rounding: {quantity}")
                return None

            if self.mode == "live" and not getattr(self.exchange, "offline", False):
                ccxt_side = "buy" if side == "long" else "sell"
                try:
                    order = self.exchange.create_order(symbol, "market", ccxt_side, rounded_qty, None, {})
                    self.logger.info(f"Live order executed: {order}")
                except Exception as e:
                    self.logger.error(f"Live order failed: {e}")
                    return None

            position = Position(
                symbol=symbol, side=side, qty=rounded_qty, entry=rounded_entry,
                sl=rounded_sl, tp=rounded_tp, lev=leverage, opened_at=time.time(),
                mode=mode, tag=tag
            )
            self.db_manager.save_position(position)
            emoji = "🟢" if side == "long" else "🔴"
            self.notifier.send(f"{emoji} Opened {side.upper()} {symbol} qty={rounded_qty} @ {rounded_entry} "
                               f"SL={rounded_sl if rounded_sl>0 else '—'} TP={rounded_tp if rounded_tp>0 else '—'}")
            return position
        except Exception as e:
            self.logger.error(f"Failed to open position for {symbol}: {e}")
            return None

    def close_position(self, position: Position, exit_price: D, reason: str = "MANUAL") -> D:
        try:
            if self.mode == "live" and not getattr(self.exchange, "offline", False):
                ccxt_side = "sell" if position.side == "long" else "buy"
                try:
                    self.exchange.create_order(position.symbol, "market", ccxt_side, position.qty, None, {})
                except Exception as e:
                    self.logger.error(f"Live close order failed for {position.symbol}: {e}")
            pnl = position.dir() * position.qty * (exit_price - position.entry)
            self.db_manager.delete_position(position.symbol)
            self.notifier.send(f"✅ Closed {position.side.upper()} {position.symbol} @ {exit_price} "
                               f"PnL={pnl} ({reason})")
            return pnl
        except Exception as e:
            self.logger.error(f"Failed to close position {position.symbol}: {e}")
            return D("0")

    def update_stop_loss(self, position: Position, new_sl: D):
        try:
            if new_sl <= 0:
                return
            position.sl = new_sl
            self.db_manager.save_position(position)
            self.notifier.send(f"🔧 Updated SL for {position.symbol} -> {new_sl}")
        except Exception as e:
            self.logger.error(f"Failed to update SL for {position.symbol}: {e}")

    def update_take_profit(self, position: Position, new_tp: D):
        try:
            if new_tp <= 0:
                return
            position.tp = new_tp
            self.db_manager.save_position(position)
            self.notifier.send(f"🔧 Updated TP for {position.symbol} -> {new_tp}")
        except Exception as e:
            self.logger.error(f"Failed to update TP for {position.symbol}: {e}")

# ====================== BOT ORCHESTRATOR ======================
class OmegaXBot:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mode = MODE
        self.notifier = Notifier(TELEGRAM_ENABLED)
        self.exchange = ExchangeWrapper(self.mode)
        self.db_manager = DatabaseManager(DB_PATH)
        self.strategy = Strategy()
        # Ensure BTC/USDT is available for pair logic
        symbols = list(dict.fromkeys(SYMBOLS + (["BTC/USDT"] if "BTC/USDT" not in SYMBOLS else [])))
        self.data_manager = DataManager(self.exchange, symbols)
        self.indicator_cache = IndicatorCache()
        self.health_monitor = HealthMonitor()
        self.exec = ExecutionEngine(self.exchange, self.db_manager, self.notifier, self.mode)
        self._shutdown_event = ThreadEvent()
        self._running = False
        self.symbols = SYMBOLS  # tradable list (BTC included only for data)
        self.risk_manager = RiskManager(INITIAL_BALANCE)
        self._restore_risk_state()
        self.portfolio = Portfolio()
        self._restore_positions()
        self._last_error_notification = 0.0

    def _restore_risk_state(self):
        state = self.db_manager.load_risk_state()
        if not state:
            return
        try:
            with self.risk_manager._lock:
                self.risk_manager.balance = safe_decimal(state['balance'])
                self.risk_manager.equity_peak = safe_decimal(state['equity_peak'])
                self.risk_manager.daily_pnl = safe_decimal(state['daily_pnl'])
                self.risk_manager.loss_bucket = safe_decimal(state['loss_bucket'])
                self.risk_manager.consec_losses = int(state['consec_losses'])
                self.risk_manager.paused_until = float(state['paused_until'])
                self.risk_manager._risk_off_until = float(state['risk_off_until'])
                self.risk_manager.day_anchor = int(state['day_anchor'])
        except Exception as e:
            self.logger.error(f"Failed to restore risk state: {e}")

    def _restore_positions(self):
        positions = self.db_manager.load_positions()
        for pos in positions.values():
            self.portfolio.add_position(pos)

    def _update_data(self):
        for symbol in self.data_manager.symbols:
            for tf in ["5m", "15m", "1h"]:
                if not self.data_manager.is_data_fresh(symbol, tf, max_age=300):
                    self.data_manager.update_data(symbol, tf)

    def _get_current_prices(self) -> Dict[str, D]:
        prices: Dict[str, D] = {}
        for symbol in self.data_manager.symbols:
            try:
                ticker = self.exchange.fetch_ticker(symbol)
                price = ticker.get('last') or ticker.get('close') or ticker.get('ask') or ticker.get('bid')
                if price:
                    prices[symbol] = safe_decimal(price)
            except Exception as e:
                self.logger.warning(f"Failed to get price for {symbol}: {e}")
                df = self.data_manager.get_data(symbol, "5m")
                if not df.empty:
                    prices[symbol] = safe_decimal(df["close"].iloc[-1])
        return prices

    def _update_trailing_stop(self, position: Position, current_price: D):
        try:
            df = self.data_manager.get_data(position.symbol, "5m")
            if df.empty or len(df) < 20:
                return
            atr_val = AverageTrueRange(df["high"], df["low"], df["close"], 14).average_true_range().iloc[-1]
            atr_decimal = safe_decimal(atr_val)
            if atr_decimal <= 0:
                return
            unrealized_pnl = position.unrealized_pnl(current_price)
            risk_amount = abs(position.entry - position.sl) * position.qty if position.sl > 0 else D("0")
            if risk_amount <= 0:
                return
            r_multiple = unrealized_pnl / risk_amount
            if r_multiple >= D("3"):
                trail_mult = D("0.7")
            elif r_multiple >= D("2"):
                trail_mult = D("1.0")
            else:
                trail_mult = D("1.4")
            if position.side == "long":
                new_sl = current_price - (atr_decimal * trail_mult)
                if (position.sl <= 0) or (new_sl > position.sl):
                    self.exec.update_stop_loss(position, new_sl)
            else:
                new_sl = current_price + (atr_decimal * trail_mult)
                if (position.sl <= 0) or (new_sl < position.sl):
                    self.exec.update_stop_loss(position, new_sl)
        except Exception as e:
            self.logger.error(f"Failed to update trailing stop for {position.symbol}: {e}")

    def _manage_positions(self, prices: Dict[str, D]):
        positions_to_close: List[Tuple[Position, D, str]] = []
        for position in self.portfolio.get_all_positions():
            try:
                current_price = prices.get(position.symbol)
                if not current_price:
                    continue
                if position.time_limit and time.time() >= position.time_limit:
                    positions_to_close.append((position, current_price, "TIME_LIMIT"))
                    continue
                if position.sl > 0:
                    if (position.side == "long" and current_price <= position.sl) or \
                       (position.side == "short" and current_price >= position.sl):
                        positions_to_close.append((position, current_price, "STOP_LOSS"))
                        continue
                if position.tp > 0:
                    if (position.side == "long" and current_price >= position.tp) or \
                       (position.side == "short" and current_price <= position.tp):
                        positions_to_close.append((position, current_price, "TAKE_PROFIT"))
                        continue
                self._update_trailing_stop(position, current_price)
            except Exception as e:
                self.logger.error(f"Error managing position {position.symbol}: {e}")

        for position, price, reason in positions_to_close:
            try:
                pnl = self.exec.close_position(position, price, reason)
                self.portfolio.remove_position(position.symbol)
                new_balance = self.risk_manager.balance + pnl
                self.risk_manager.update_balance(new_balance)
                self.db_manager.save_risk_state(self.risk_manager)
            except Exception as e:
                self.logger.error(f"Failed to close position {position.symbol}: {e}")

    def _analyze_symbol(self, symbol: str, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame, current_price: D) -> Optional[Tuple[str, D, D, D, D]]:
        try:
            indicators = self.indicator_cache.get_indicators(symbol, df5, df15, df1h)
            if not indicators:
                return None
            regime, vol_spike = self.strategy.detect_regime(df5, df15)
            btc_df5 = self.data_manager.get_data("BTC/USDT", "5m")
            cs_rank_map = self.strategy.cross_sectional_mom_rank({symbol: df1h})
            cs_rank = cs_rank_map.get(symbol, 0.0)
            alpha_scores = self.strategy.alpha_scores(symbol, df5, df15, df1h, btc_df5, cs_rank, 0.0)
            if not alpha_scores:
                return None
            long_score = sum(scores[0] for scores in alpha_scores.values()) / len(alpha_scores)
            short_score = sum(scores[1] for scores in alpha_scores.values()) / len(alpha_scores)
            threshold = 0.8 if vol_spike else 0.6
            side = None
            confidence = D("0")
            if long_score > threshold and long_score > short_score:
                side = "long"
                confidence = safe_decimal(min(max(0.0, long_score), 1.0))
            elif short_score > threshold and short_score > long_score:
                side = "short"
                confidence = safe_decimal(min(max(0.0, short_score), 1.0))
            if not side:
                return None
            atr_series = indicators.get('atr')
            if atr_series is None or atr_series.empty:
                return None
            current_atr = safe_decimal(atr_series.iloc[-1])
            if current_atr <= 0:
                return None
            if "Trending" in regime:
                sl_mult = D("1.5")
                tp_mult = D("2.0")
            else:
                sl_mult = D("1.0")
                tp_mult = D("1.5")
            if side == "long":
                stop_loss = current_price - (current_atr * sl_mult)
                take_profit = current_price + (current_atr * tp_mult)
            else:
                stop_loss = current_price + (current_atr * sl_mult)
                take_profit = current_price - (current_atr * tp_mult)
            if stop_loss <= 0 or take_profit <= 0:
                return None
            return side, current_price, stop_loss, take_profit, confidence
        except Exception as e:
            self.logger.error(f"Analysis failed for {symbol}: {e}")
            return None

    def _scan_for_opportunities(self, prices: Dict[str, D]):
        if self.risk_manager.risk_off:
            return
        open_positions = len(self.portfolio.get_open_positions())
        if open_positions >= MAX_CONCURRENT_POS:
            return
        for symbol in self.symbols:
            try:
                if self.portfolio.get_position(symbol):
                    continue
                if not self.risk_manager.can_trade(symbol):
                    continue
                current_price = prices.get(symbol)
                if not current_price:
                    continue
                df5 = self.data_manager.get_data(symbol, "5m")
                df15 = self.data_manager.get_data(symbol, "15m")
                df1h = self.data_manager.get_data(symbol, "1h")
                if df5.empty or df15.empty or df1h.empty:
                    continue
                signal = self._analyze_symbol(symbol, df5, df15, df1h, current_price)
                if not signal:
                    continue
                side, entry_price, stop_loss, take_profit, confidence = signal
                position_size = self.risk_manager.position_size(symbol, entry_price, stop_loss, RISK_PER_TRADE)
                position_size *= confidence
                min_notional = self.exchange.get_min_notional(symbol)
                if position_size * entry_price < min_notional:
                    continue
                position = self.exec.open_position(
                    symbol=symbol,
                    side=side,
                    quantity=position_size,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    leverage=D("10"),
                    mode="directional",
                    tag="strategy"
                )
                if position:
                    self.portfolio.add_position(position)
                    self.risk_manager.note_trade_opened(symbol)
                    open_positions += 1
                    if open_positions >= MAX_CONCURRENT_POS:
                        break
            except Exception as e:
                self.logger.error(f"Error scanning {symbol}: {e}")

    def _periodic_tasks(self):
        try:
            self.db_manager.save_risk_state(self.risk_manager)
            self.health_monitor.cleanup_memory()
            self._send_periodic_report()
        except Exception as e:
            self.logger.error(f"Periodic tasks failed: {e}")

    def _send_periodic_report(self):
        try:
            open_positions = self.portfolio.get_open_positions()
            report = (
                f"📊 Status Report\n"
                f"Balance: ${float(self.risk_manager.balance):.2f}\n"
                f"Drawdown: {float(self.risk_manager.drawdown):.2%}\n"
                f"Daily PnL: ${float(self.risk_manager.daily_pnl):.2f}\n"
                f"Open Positions: {len(open_positions)}\n"
                f"Risk Off: {'Yes' if self.risk_manager.risk_off else 'No'}"
            )
            self.notifier.send(report)
        except Exception as e:
            self.logger.error(f"Failed to send report: {e}")

    def run(self):
        self._running = True
        self.logger.info("Starting trading bot...")

        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, shutting down...")
            self.stop()
        try:
            import signal
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        except Exception:
            pass

        last_data_update = 0.0
        last_periodic_task = 0.0
        try:
            while self._running and not self._shutdown_event.is_set():
                loop_start = time.time()
                try:
                    if time.time() - last_data_update > 60:
                        self._update_data()
                        last_data_update = time.time()
                    prices = self._get_current_prices()
                    self._manage_positions(prices)
                    self._scan_for_opportunities(prices)
                    if time.time() - last_periodic_task > 900:
                        self._periodic_tasks()
                        last_periodic_task = time.time()
                except HardStopTriggered as e:
                    self.logger.critical(f"Hard stop triggered: {e}")
                    self.notifier.send_critical(f"🛑 HARD STOP: {e}")
                    for position in self.portfolio.get_all_positions():
                        try:
                            price = prices.get(position.symbol, position.entry)
                            self.exec.close_position(position, price, "HARD_STOP")
                            self.portfolio.remove_position(position.symbol)
                        except Exception as close_error:
                            self.logger.error(f"Failed to close {position.symbol}: {close_error}")
                    break
                except Exception as e:
                    self.logger.error(f"Loop error: {e}")
                    self.health_monitor.record_error(e)
                    if time.time() - self._last_error_notification > 300:
                        self.notifier.send(f"⚠️ Trading loop error: {str(e)[:100]}")
                        self._last_error_notification = time.time()
                loop_time = time.time() - loop_start
                sleep_time = max(0.1, 5.0 - loop_time + random.uniform(-0.5, 0.5))
                if self._shutdown_event.wait(sleep_time):
                    break
        except Exception as e:
            self.logger.critical(f"Fatal error in main loop: {e}")
            self.notifier.send_critical(f"💥 FATAL ERROR: {e}")
            raise
        finally:
            self._cleanup()

    def stop(self):
        self.logger.info("Stopping trading bot...")
        self._running = False
        self._shutdown_event.set()

    def _cleanup(self):
        try:
            self.db_manager.save_risk_state(self.risk_manager)
            backup_path = f"{DB_PATH}.backup.{int(time.time())}"
            self.db_manager.create_backup(backup_path)
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")

# ====================== ENTRY POINT ======================
def main():
    setup_logging()
    try:
        bot = OmegaXBot()
        bot.run()
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.critical(f"Bot crashed: {e}")
        raise

if __name__ == "__main__":
    main()