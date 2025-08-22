# -*- coding: utf-8 -*-
# OmegaX Trading Bot - Compact Version (Python 3.13.4 compatible)
# All original logic preserved in condensed form

import os, time, random, logging, signal, threading, sqlite3, gc, json
... from threading import RLock, Event as ThreadEvent, Lock
... from dataclasses import dataclass
... from typing import Dict, List, Optional, Tuple, Any
... from datetime import datetime, timezone
... from decimal import Decimal, getcontext, ROUND_DOWN, ROUND_HALF_UP
... from contextlib import contextmanager
... import numpy as np, pandas as pd, requests, ccxt
... from flask import Flask, jsonify
... from ta.trend import EMAIndicator, MACD, ADXIndicator, SMAIndicator
... from ta.volatility import BollingerBands, AverageTrueRange
... from ta.momentum import RSIIndicator
... from ta.volume import VolumeWeightedAveragePrice
... 
... # ====================== CONFIG ======================
... getcontext().prec = 34
... D = Decimal
... 
... def setup_logging():
...     level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
...     handlers = [logging.StreamHandler()]
...     try:
...         from logging.handlers import RotatingFileHandler
...         handlers.append(RotatingFileHandler("trading_bot.log", maxBytes=10*1024*1024, backupCount=5))
...     except: pass
...     logging.basicConfig(level=level, handlers=handlers, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", force=True)
... 
... def validate_config() -> str:
...     mode = os.environ.get('MODE', 'paper').lower()
...     if mode == 'live':
...         for var in ['BINANCE_API_KEY', 'BINANCE_API_SECRET']:
...             if not os.environ.get(var, '').strip():
...                 raise ValueError(f"Required {var} not set for live trading")
...     return mode
... 
... MODE = validate_config()
... 
... # Constants with validation
... INITIAL_BALANCE = D(os.environ.get("INITIAL_BALANCE", "3000"))
... RISK_PER_TRADE = max(D("0.001"), min(D("0.05"), D(os.environ.get("RISK_PER_TRADE", "0.005"))))
MAX_TOTAL_RISK = max(D("0.01"), min(D("0.2"), D(os.environ.get("MAX_TOTAL_RISK", "0.06"))))
MAX_CONCURRENT_POS = max(1, min(20, int(os.environ.get("MAX_CONCURRENT_POS", "8"))))
MAX_DRAWDOWN = max(D("0.02"), min(D("0.3"), D(os.environ.get("MAX_DRAWDOWN", "0.08"))))
HTTP_TIMEOUT = max(5, min(30, int(os.environ.get("HTTP_TIMEOUT", "10"))))
RETRY_LIMIT = max(1, min(10, int(os.environ.get("RETRY_LIMIT", "3"))))

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_ENABLED = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID and len(TELEGRAM_TOKEN) > 20)

DB_PATH = os.environ.get("DB_PATH", "state.db")
SYMBOLS = list(dict.fromkeys([s.strip() for s in os.environ.get("SYMBOLS", "BTC/USDT,ETH/USDT").split(",") if s.strip()]))
PORT = int(os.environ.get("PORT", "10000"))

DATA_MAX_AGE = 300
PRICE_MAX_AGE = 60

# ====================== UTILS ======================
def safe_decimal(value: Any, default: D = D("0")) -> D:
    try:
        if isinstance(value, D) and value.is_finite(): return value
        if value is None or str(value).lower() in ['nan', 'inf', '-inf', '']: return default
        result = D(str(value))
        return result if result.is_finite() else default
    except: return default

def quantize_step(x: D, step: D, rounding=ROUND_DOWN) -> D:
    try:
        return (x / step).to_integral_value(rounding=rounding) * step if step > 0 and x.is_finite() and step.is_finite() else x
    except: return x

def validate_dataframe(df: pd.DataFrame, min_rows: int = 10) -> bool:
    if df.empty or len(df) < min_rows: return False
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in required_cols): return False
    return not any(df[col].isna().all() or (df[col] <= 0).all() for col in required_cols)

def is_data_fresh(timestamp: float, max_age: int = DATA_MAX_AGE) -> bool:
    return time.time() - timestamp < max_age

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

# ====================== NOTIFIER ======================
class Notifier:
    def __init__(self, enabled: bool):
        self.enabled = enabled
        self._last_send, self._min_interval = 0.0, 2.0
        self._message_queue, self._lock = [], RLock()
        self.logger = logging.getLogger(self.__class__.__name__)

    def send(self, msg: str, critical: bool = False):
        if not self.enabled:
            self.logger.info(f"Notification: {msg}")
            return
        with self._lock:
            self._message_queue.append((msg, critical, time.time()))
            if len(self._message_queue) > 100: self._message_queue = self._message_queue[-100:]
            self._process_queue()

    def _process_queue(self):
        if not self._message_queue or time.time() - self._last_send < self._min_interval: return
        critical_msgs = [(i, msg, crit, ts) for i, (msg, crit, ts) in enumerate(self._message_queue) if crit]
        idx, msg, critical, timestamp = critical_msgs[0] if critical_msgs else (0, *self._message_queue[0])
        
        if self._send_telegram(msg):
            self._last_send = time.time()
            self._message_queue.pop(idx)
        elif idx < len(self._message_queue):
            old_msg, _, old_ts = self._message_queue[idx]
            self._message_queue[idx] = (old_msg, False, old_ts)

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

# ====================== STRATEGY ======================
class Strategy:
    def __init__(self): self.logger = logging.getLogger(self.__class__.__name__)
    
    def detect_regime(self, df5: pd.DataFrame, df15: pd.DataFrame) -> Tuple[str, bool]:
        try:
            if not validate_dataframe(df5, 50) or not validate_dataframe(df15, 20): return "Unknown", False
            atr_series = AverageTrueRange(df5["high"], df5["low"], df5["close"], 14).average_true_range()
            if atr_series.empty or atr_series.isna().all(): return "Unknown", False
            current_atr, avg_atr = float(atr_series.iloc[-1]), float(atr_series.tail(50).dropna().mean())
            if np.isnan(current_atr) or current_atr <= 0 or avg_atr <= 0: return "Unknown", False
            vol_spike = current_atr > avg_atr * 1.5
            adx_series = ADXIndicator(df5["high"], df5["low"], df5["close"], 14).adx()
            if adx_series.empty or adx_series.isna().all(): return "Unknown", False
            current_adx = float(adx_series.iloc[-1])
            if np.isnan(current_adx): return "Unknown", False
            regime = ("Trending" if current_adx > 25 else "Ranging") + (" High Volatility" if vol_spike else "")
            return regime, vol_spike
        except Exception as e:
            self.logger.warning(f"Regime detection failed: {e}")
            return "Unknown", False
    
    def cross_sectional_mom_rank(self, df_map: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        try:
            mom_scores = {}
            for symbol, df in df_map.items():
                if not validate_dataframe(df, 50):
                    mom_scores[symbol] = 0.0
                    continue
                closes = df["close"].dropna()
                if len(closes) < 30:
                    mom_scores[symbol] = 0.0
                    continue
                returns_1w = float(closes.iloc[-1] / closes.iloc[-7] - 1) if len(closes) >= 7 else 0.0
                returns_1m = float(closes.iloc[-1] / closes.iloc[-30] - 1) if len(closes) >= 30 else 0.0
                if not all(np.isfinite([returns_1w, returns_1m])): returns_1w = returns_1m = 0.0
                mom_scores[symbol] = max(-1.0, min(1.0, returns_1w * 0.3 + returns_1m * 0.7))
            
            if not mom_scores: return {}
            sorted_scores = sorted(mom_scores.items(), key=lambda x: x[1], reverse=True)
            n = len(sorted_scores)
            return {symbol: max(-1.0, min(1.0, (2 * (n - i - 1) / (n - 1) - 1) if n > 1 else 0.0)) 
                   for i, (symbol, _) in enumerate(sorted_scores)}
        except Exception as e:
            self.logger.warning(f"Cross-sectional momentum failed: {e}")
            return {}
    
    def alpha_scores(self, symbol: str, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame, 
                    btc_df5: pd.DataFrame, cs_rank: float, funding_rate: float) -> Dict[str, Tuple[float, float]]:
        try:
            scores = {}
            if not all(validate_dataframe(df, 20) for df in [df5, df15, df1h]): return scores
            price = float(df5["close"].iloc[-1])
            if not np.isfinite(price) or price <= 0: return scores

            # Trend following
            try:
                ema_fast = float(EMAIndicator(df5["close"], 12).ema_indicator().iloc[-1])
                ema_slow = float(EMAIndicator(df5["close"], 26).ema_indicator().iloc[-1])
                if all(np.isfinite([ema_fast, ema_slow]) and x > 0 for x in [ema_fast, ema_slow]):
                    scores["trend"] = (1.0 if price > ema_fast > ema_slow else 0.0, 1.0 if price < ema_fast < ema_slow else 0.0)
                else: scores["trend"] = (0.0, 0.0)
            except: scores["trend"] = (0.0, 0.0)
            
            # Mean reversion
            try:
                bb = BollingerBands(df5["close"], 20, 2)
                bb_upper, bb_lower = float(bb.bollinger_hband().iloc[-1]), float(bb.bollinger_lband().iloc[-1])
                if all(np.isfinite([bb_upper, bb_lower]) and x > 0 for x in [bb_upper, bb_lower]):
                    denom = bb_upper - bb_lower
                    if denom > 0:
                        bb_position = max(0.0, min(1.0, (price - bb_lower) / denom))
                        scores["meanrev"] = (max(0.0, 1 - bb_position * 2) if bb_position < 0.3 else 0.0,
                                           max(0.0, bb_position * 2 - 1) if bb_position > 0.7 else 0.0)
                    else: scores["meanrev"] = (0.0, 0.0)
                else: scores["meanrev"] = (0.0, 0.0)
            except: scores["meanrev"] = (0.0, 0.0)
            
            # VWAP, Donchian, Cross-sectional, Carry
            try:
                vwap = float(VolumeWeightedAveragePrice(df5["high"], df5["low"], df5["close"], df5["volume"]).volume_weighted_average_price().iloc[-1])
                scores["vwap"] = (1.0 if np.isfinite(vwap) and vwap > 0 and price > vwap * 1.001 else 0.0,
                                                                1.0 if np.isfinite(vwap) and vwap > 0 and price < vwap * 0.999 else 0.0)
            except: scores["vwap"] = (0.0, 0.0)
            
            try:
                don_high, don_low = float(df5["high"].rolling(20).max().iloc[-1]), float(df5["low"].rolling(20).min().iloc[-1])
                if all(np.isfinite([don_high, don_low]) and x > 0 for x in [don_high, don_low]):
                    scores["donchian"] = (1.0 if price >= don_high * 0.999 else 0.0, 1.0 if price <= don_low * 1.001 else 0.0)
                else: scores["donchian"] = (0.0, 0.0)
            except: scores["donchian"] = (0.0, 0.0)
            
            if np.isfinite(cs_rank):
                cs_rank = max(-1.0, min(1.0, cs_rank))
                scores["xsmom"] = (max(0.0, cs_rank) if cs_rank > 0.3 else 0.0, max(0.0, -cs_rank) if cs_rank < -0.3 else 0.0)
            else: scores["xsmom"] = (0.0, 0.0)
            
            if np.isfinite(funding_rate):
                funding_rate = max(-0.01, min(0.01, funding_rate))
                scores["carry"] = (1.0 if funding_rate < -0.0001 else 0.0, 1.0 if funding_rate > 0.0001 else 0.0)
            else: scores["carry"] = (0.0, 0.0)
            
            # Pair trading
            try:
                if validate_dataframe(btc_df5, 20) and len(btc_df5) >= len(df5):
                    asset_returns = df5["close"].pct_change().tail(20).dropna()
                    btc_returns = btc_df5["close"].pct_change().tail(20).dropna()
                    min_len = min(len(asset_returns), len(btc_returns))
                    if min_len >= 10:
                        asset_ret, btc_ret = asset_returns.tail(min_len).values, btc_returns.tail(min_len).values
                        if (np.isfinite(asset_ret).all() and np.isfinite(btc_ret).all() and 
                            np.std(asset_ret) > 0 and np.std(btc_ret) > 0):
                            correlation = np.corrcoef(asset_ret, btc_ret)[0, 1]
                            if np.isfinite(correlation) and abs(correlation) > 0.5 and len(df5) >= 10 and len(btc_df5) >= 10:
                                btc_momentum = float(btc_df5["close"].iloc[-1] / btc_df5["close"].iloc[-10] - 1)
                                asset_momentum = float(df5["close"].iloc[-1] / df5["close"].iloc[-10] - 1)
                                if np.isfinite(btc_momentum) and np.isfinite(asset_momentum):
                                    relative_perf = max(-0.5, min(0.5, asset_momentum - btc_momentum))
                                    scores["pair"] = (max(0.0, -relative_perf) if relative_perf < -0.02 else 0.0,
                                                    max(0.0, relative_perf) if relative_perf > 0.02 else 0.0)
                                else: scores["pair"] = (0.0, 0.0)
                            else: scores["pair"] = (0.0, 0.0)
                        else: scores["pair"] = (0.0, 0.0)
                    else: scores["pair"] = (0.0, 0.0)
                else: scores["pair"] = (0.0, 0.0)
            except: scores["pair"] = (0.0, 0.0)
            
            return scores
        except Exception as e:
            self.logger.error(f"Alpha scores failed for {symbol}: {e}")
            return {}

# ====================== DOMAIN MODELS ======================
@dataclass
class Position:
    symbol: str; side: str; qty: D; entry: D; sl: D; tp: D; lev: D; opened_at: float
    partial: bool = False; is_hedge: bool = False; mode: str = "directional"; pyramids: int = 0
    last_add_price: Optional[D] = None; tag: str = "meta"; entry_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None; tp_order_id: Optional[str] = None
    client_order_id: Optional[str] = None; time_limit: Optional[float] = None

    def __post_init__(self):
        if self.side not in ("long", "short"): raise ValueError(f"Invalid side: {self.side}")
        if not isinstance(self.qty, D) or self.qty <= 0 or not self.qty.is_finite(): raise ValueError(f"Invalid quantity: {self.qty}")
        if not isinstance(self.entry, D) or self.entry <= 0 or not self.entry.is_finite(): raise ValueError(f"Invalid entry: {self.entry}")
        if self.sl > 0:
            if self.side == "long" and self.sl >= self.entry: raise ValueError("Long SL must be below entry")
            if self.side == "short" and self.sl <= self.entry: raise ValueError("Short SL must be above entry")
        if self.tp > 0:
            if self.side == "long" and self.tp <= self.entry: raise ValueError("Long TP must be above entry")
            if self.side == "short" and self.tp >= self.entry: raise ValueError("Short TP must be below entry")

    def dir(self) -> D: return D("1") if self.side == "long" else D("-1")
    def unrealized_pnl(self, current_price: D) -> D:
        try: return self.dir() * self.qty * (current_price - self.entry) if isinstance(current_price, D) and current_price > 0 and current_price.is_finite() else D("0")
        except: return D("0")
    def risk_amount(self) -> D:
        try: return abs(self.entry - self.sl) * self.qty if self.sl > 0 else D("0")
        except: return D("0")
    def is_expired(self) -> bool: return self.time_limit is not None and time.time() >= self.time_limit

# ====================== RISK MANAGEMENT ======================
class RiskManager:
    def __init__(self, initial_balance: D):
        if not isinstance(initial_balance, D) or initial_balance <= 0: raise ValueError(f"Invalid initial balance: {initial_balance}")
        self.initial = self.balance = self.equity_peak = initial_balance
        self.paused_until = self._risk_off_until = 0.0; self.consec_losses = 0
        self.day_anchor = self._utc_midnight_ts(); self.daily_pnl = self.loss_bucket = D("0")
        self.cooldown, self._lock = {}, RLock(); self.logger = logging.getLogger(self.__class__.__name__)

    def _utc_midnight_ts(self) -> int: return int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    def _check_day_rollover(self):
        current_midnight = self._utc_midnight_ts()
        if current_midnight > self.day_anchor:
            days_passed = (current_midnight - self.day_anchor) // 86400
            if days_passed >= 1:
                self.day_anchor, self.daily_pnl = current_midnight, D("0")
                if days_passed >= 1: self.consec_losses, self.loss_bucket = 0, self.loss_bucket * D("0.8")

    @property
    def drawdown(self) -> D:
        with self._lock: return max(D("0"), (self.equity_peak - self.balance) / self.equity_peak) if self.equity_peak > 0 else D("0")
    @property
    def risk_off(self) -> bool:
        with self._lock: return time.time() < self._risk_off_until or time.time() < self.paused_until

    def update_balance(self, new_balance: D):
        if not isinstance(new_balance, D) or not new_balance.is_finite(): raise ValueError(f"Invalid balance: {new_balance}")
        with self._lock:
            self._check_day_rollover(); old_balance, pnl_change = self.balance, new_balance - self.balance
            self.balance, self.equity_peak, self.daily_pnl = new_balance, max(self.equity_peak, new_balance), self.daily_pnl + pnl_change
            if pnl_change < 0: self.consec_losses += 1; self.loss_bucket += abs(pnl_change)
            else: self.consec_losses = 0; self.loss_bucket = max(D("0"), self.loss_bucket - min(self.loss_bucket, pnl_change * D("0.5"))) if self.loss_bucket > 0 else D("0")
        self._check_risk_limits()

    def _check_risk_limits(self):
        dd = self.drawdown
        if dd >= MAX_DRAWDOWN: raise HardStopTriggered(f"Drawdown {float(dd):.2%} exceeds limit {float(MAX_DRAWDOWN):.2%}")
        if dd >= D("0.05"):
            with self._lock: self.paused_until = max(self.paused_until, time.time() + 1800 + (float(dd) * 3600))
        with self._lock:
            daily_loss_pct = abs(self.daily_pnl) / self.balance if self.balance > 0 else D("0")
            if self.daily_pnl < 0 and daily_loss_pct >= D("0.03"):
                self._risk_off_until = max(self._risk_off_until, self.day_anchor + 86400)

    def can_trade(self, symbol: str) -> bool:
        with self._lock: return not self.risk_off and time.time() >= self.cooldown.get(symbol, 0.0) and self.balance > self.initial * D("0.1")
    def note_trade_opened(self, symbol: str): 
        with self._lock: self.cooldown[symbol] = time.time() + 300

    def position_size(self, symbol: str, entry: D, sl: D, risk_fraction: D) -> D:
        try:
            if not all(isinstance(x, D) and x.is_finite() for x in [entry, sl, risk_fraction]) or entry <= 0 or sl <= 0 or entry == sl or not 0 < risk_fraction <= D("0.1"): return D("0")
            with self._lock:
                if self.balance <= 0: return D("0")
                risk_amount, price_risk = self.balance * risk_fraction, abs(entry - sl)
                if price_risk <= 0: return D("0")
                position_size = risk_amount / price_risk
                max_position_value = self.balance * D("10")
                return max(D("0"), min(position_size, max_position_value / entry))
        except: return D("0")

# ====================== EXCHANGE WRAPPER ======================
class ExchangeWrapper:
    def __init__(self, mode: str):
        self.logger, self.mode = logging.getLogger(self.__class__.__name__), mode
        self._init_exchange(); self.circuit_breaker = CircuitBreaker()
        self._steps_cache, self._min_notional_cache, self._market_status_cache = {}, {}, {}
        self._last_market_check = 0.0; self._load_market_info()

    def _init_exchange(self):
        try:
            config = {"apiKey": os.environ.get("BINANCE_API_KEY"), "secret": os.environ.get("BINANCE_API_SECRET"),
                     "options": {"defaultType": "future", "adjustForTimeDifference": True}, "enableRateLimit": True, "timeout": HTTP_TIMEOUT * 1000}
            if self.mode == "paper": config["sandbox"] = True
            self.exchange = ccxt.binance(config); self.exchange.load_markets()
            futures_markets = [s for s, m in self.exchange.markets.items() if m.get('type') == 'future']
            if not futures_markets: raise ConfigurationError("No futures markets found")
        except Exception as e: raise ConfigurationError(f"Exchange init failed: {e}")

    def _load_market_info(self):
        try:
            for symbol, market in self.exchange.markets.items():
                if market.get('type') != 'future': continue
                precision, limits = market.get("precision", {}), market.get("limits", {})
                price_prec, amount_prec = precision.get("price"), precision.get("amount")
                tick_size = D(f"1e-{price_prec}") if price_prec is not None else safe_decimal(limits.get("price", {}).get("min"), D("0.01"))
                step_size = D(f"1e-{amount_prec}") if amount_prec is not None else safe_decimal(limits.get("amount", {}).get("min"), D("0.001"))
                if tick_size <= 0: tick_size = D("0.01")
                if step_size <= 0: step_size = D("0.001")
                self._steps_cache[symbol] = (tick_size, step_size)
                min_cost = limits.get("cost", {}).get("min")
                if min_cost is None:
                    info = market.get("info", {})
                    min_cost = info.get("minNotional") or info.get("quoteOrderQtyMin") if isinstance(info, dict) else None
                self._min_notional_cache[symbol] = safe_decimal(min_cost, D("5"))
                self._market_status_cache[symbol] = market.get('active', True)
        except Exception as e: raise ConfigurationError(f"Market info loading failed: {e}")

    def _with_retries(self, func, *args, **kwargs):
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
                        try: self.exchange.load_markets(); self._load_market_info(); self._last_market_check = time.time()
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
        if rounded_amount <= 0: raise ValueError(f"Amount too small: {amount}")
        min_notional = self._min_notional_cache.get(symbol, D("5"))
        notional = rounded_amount * (price if price else safe_decimal(self.fetch_ticker(symbol).get('last', 0)))
        if notional < min_notional: raise InsufficientFundsError(f"Notional {notional} below minimum {min_notional}")
        price_float = float(quantize_step(price, tick_size)) if price else None
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
            if not validate_dataframe(df): return False
            for col in ["open", "high", "low", "close", "volume"]: df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.dropna()
            if len(df) < 10: return False
            invalid_ohlc = ((df['high'] < df['low']) | (df['high'] < df['open']) | (df['high'] < df['close']) | 
                           (df['low'] > df['open']) | (df['low'] > df['close']) | (df['volume'] < 0))
            if invalid_ohlc.any(): df = df[~invalid_ohlc]
            if len(df) < 10: return False
            self.data[symbol][timeframe], self.last_update[symbol][timeframe], self.update_errors[symbol] = df, time.time(), 0
            return True
        except Exception as e:
            self.update_errors[symbol] += 1; self.logger.error(f"Data update failed {symbol} {timeframe}: {e}")
            if self.update_errors[symbol] > 5: time.sleep(60); self.update_errors[symbol] = 0
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
        self.cache, self.max_size, self.access_times = {}, max_size, {}
        self._lock, self.hit_count, self.miss_count = RLock(), 0, 0

    def get_indicators(self, symbol: str, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame) -> Dict:
        if not all(validate_dataframe(df, 20) for df in [df5, df15, df1h]): return {}
        try: cache_key = f"{symbol}_{int(df5['timestamp'].iloc[-1])}_{int(df15['timestamp'].iloc[-1])}_{int(df1h['timestamp'].iloc[-1])}"
        except: return {}
        with self._lock:
            if cache_key in self.cache:
                self.access_times[cache_key], self.hit_count = time.time(), self.hit_count + 1
                return self.cache[cache_key].copy()
            self.miss_count += 1; indicators = self._calculate_indicators(df5, df15, df1h)
            if indicators:
                self.cache[cache_key], self.access_times[cache_key] = indicators, time.time()
                if len(self.cache) > self.max_size: self._cleanup_cache()
            return indicators

    def _calculate_indicators(self, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame) -> Dict:
        indicators = {}
        try:
            for name, func in [('ema_12', lambda: EMAIndicator(df5["close"], 12).ema_indicator()),
                              ('ema_26', lambda: EMAIndicator(df5["close"], 26).ema_indicator()),
                              ('rsi', lambda: RSIIndicator(df5["close"], 14).rsi()),
                              ('atr', lambda: AverageTrueRange(df5["high"], df5["low"], df5["close"], 14).average_true_range())]:
                try:
                    result = func()
                    if not result.empty and not result.isna().all(): indicators[name] = result
                except: pass
            try:
                bb = BollingerBands(df5["close"], 20, 2)
                for name, func in [('bb_upper', bb.bollinger_hband), ('bb_lower', bb.bollinger_lband), ('bb_middle', bb.bollinger_mavg)]:
                    result = func()
                    if not result.empty and not result.isna().all(): indicators[name] = result
            except: pass
            try:
                macd_obj = MACD(df5["close"])
                for name, func in [('macd', macd_obj.macd), ('macd_signal', macd_obj.macd_signal)]:
                    result = func()
                    if not result.empty and not result.isna().all(): indicators[name] = result
            except: pass
            try:
                vwap = VolumeWeightedAveragePrice(df5["high"], df5["low"], df5["close"], df5["volume"]).volume_weighted_average_price()
                if not vwap.empty and not vwap.isna().all(): indicators['vwap'] = vwap
            except: pass
            try:
                indicators['don_high'], indicators['don_low'] = df5["high"].rolling(20).max(), df5["low"].rolling(20).min()
            except: pass
            try:
                indicators['sma_50_1h'] = SMAIndicator(df1h["close"], 50).sma_indicator() if len(df1h) >= 50 else pd.Series([np.nan] * len(df1h), index=df1h.index)
            except: indicators['sma_50_1h'] = pd.Series([np.nan] * len(df1h), index=df1h.index)
            return indicators
        except: return {}

    def _cleanup_cache(self):
        try:
            sorted_items = sorted(self.access_times.items(), key=lambda x: x[1])
            to_remove = max(1, int(len(sorted_items) * 0.3))
            for key, _ in sorted_items[:to_remove]:
                self.cache.pop(key, None); self.access_times.pop(key, None)
        except: pass

# ====================== DATABASE MANAGER ======================
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path, self.logger = db_path, logging.getLogger(self.__class__.__name__)
        self._connection_pool_lock = Lock(); self._init_database()

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
                if not conn.execute("SELECT COUNT(*) FROM risk_state WHERE id=1").fetchone()[0]:
                    now, midnight_ts = time.time(), int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
                    conn.execute("INSERT INTO risk_state (id,balance,equity_peak,daily_pnl,loss_bucket,consec_losses,paused_until,risk_off_until,day_anchor,updated_at) VALUES (1,?,?,'0','0',0,0,0,?,?)", 
                                (str(INITIAL_BALANCE), str(INITIAL_BALANCE), midnight_ts, now))
                conn.commit()
        except Exception as e: raise ConfigurationError(f"Database init failed: {e}")

    @contextmanager
    def _get_connection(self):
        conn = None
        try:
            with self._connection_pool_lock:
                conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
                conn.execute("PRAGMA busy_timeout=30000; PRAGMA temp_store=MEMORY")
            yield conn
        except Exception as e:
            if conn: 
                try: conn.rollback()
                except: pass
            raise e
        finally:
            if conn:
                try: conn.close()
                except: pass

    def save_risk_state(self, risk_manager) -> bool:
        try:
            with self._get_connection() as conn, risk_manager._lock:
                data = (str(risk_manager.balance), str(risk_manager.equity_peak), str(risk_manager.daily_pnl), str(risk_manager.loss_bucket),
                       risk_manager.consec_losses, risk_manager.paused_until, risk_manager._risk_off_until, risk_manager.day_anchor, time.time())
                for key in data[:4]:
                    if not np.isfinite(float(key)): return False
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute("UPDATE risk_state SET balance=?,equity_peak=?,daily_pnl=?,loss_bucket=?,consec_losses=?,paused_until=?,risk_off_until=?,day_anchor=?,updated_at=? WHERE id=1", data)
                    if conn.total_changes == 0: raise sqlite3.Error("No rows affected")
                    conn.execute("COMMIT"); return True
                except Exception as e: conn.execute("ROLLBACK"); raise e
        except Exception as e: self.logger.error(f"Save risk state failed: {e}"); return False

    def load_risk_state(self) -> Dict:
        try:
            with self._get_connection() as conn:
                                row = conn.execute("SELECT balance,equity_peak,daily_pnl,loss_bucket,consec_losses,paused_until,risk_off_until,day_anchor,updated_at FROM risk_state WHERE id=1").fetchone()
                if row:
                    data = {'balance': row[0], 'equity_peak': row[1], 'daily_pnl': row[2], 'loss_bucket': row[3], 'consec_losses': row[4], 'paused_until': row[5], 'risk_off_until': row[6], 'day_anchor': row[7], 'updated_at': row[8]}
                    for key in ['balance', 'equity_peak', 'daily_pnl', 'loss_bucket']:
                        try:
                            if not np.isfinite(float(data[key])): data[key] = "0"
                        except: data[key] = "0"
                    return data
                return {}
        except Exception as e: self.logger.error(f"Load risk state failed: {e}"); return {}

    def save_position(self, position: Position) -> bool:
        try:
            position.__post_init__()
            with self._get_connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute("INSERT OR REPLACE INTO positions (symbol,side,qty,entry,sl,tp,lev,opened_at,partial,is_hedge,mode,tag,pyramids,last_add_price,entry_order_id,sl_order_id,tp_order_id,client_order_id,time_limit,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                                (position.symbol, position.side, str(position.qty), str(position.entry), str(position.sl), str(position.tp), str(position.lev), position.opened_at, int(position.partial), int(position.is_hedge), position.mode, position.tag, position.pyramids, str(position.last_add_price) if position.last_add_price else None, position.entry_order_id, position.sl_order_id, position.tp_order_id, position.client_order_id, position.time_limit, time.time()))
                    conn.execute("COMMIT"); return True
                except Exception as e: conn.execute("ROLLBACK"); raise e
        except Exception as e: self.logger.error(f"Save position failed {position.symbol}: {e}"); return False

    def delete_position(self, symbol: str) -> bool:
        try:
            with self._get_connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
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
                        positions[data['symbol']] = Position(symbol=data['symbol'], side=data['side'], qty=safe_decimal(data['qty']), entry=safe_decimal(data['entry']), sl=safe_decimal(data['sl']), tp=safe_decimal(data['tp']), lev=safe_decimal(data['lev']), opened_at=float(data['opened_at']), partial=bool(data['partial']), is_hedge=bool(data['is_hedge']), mode=data['mode'], tag=data['tag'], pyramids=int(data.get('pyramids', 0)), last_add_price=safe_decimal(data['last_add_price']) if data['last_add_price'] else None, entry_order_id=data.get('entry_order_id'), sl_order_id=data.get('sl_order_id'), tp_order_id=data.get('tp_order_id'), client_order_id=data.get('client_order_id'), time_limit=data.get('time_limit'))
                    except Exception as e: self.logger.error(f"Load position failed {data.get('symbol', 'unknown')}: {e}")
        except Exception as e: self.logger.error(f"Load positions failed: {e}")
        return positions

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
        self.error_types, self._lock = {}, RLock()

    def record_error(self, error: Exception, error_type: str = "general"):
        with self._lock: self.error_count += 1; self.last_error_time = time.time(); self.error_types[error_type] = self.error_types.get(error_type, 0) + 1

    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            uptime = time.time() - self.start_time
            try: import psutil; memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
            except: memory_mb = 0.0
            error_rate = self.error_count / (uptime / 3600) if uptime > 0 else 0
            status = 'healthy' if self.error_count < 5 and error_rate < 1.0 else ('degraded' if self.error_count < 20 and error_rate < 5.0 else 'unhealthy')
            return {'status': status, 'uptime_seconds': uptime, 'error_count': self.error_count, 'error_rate_per_hour': error_rate, 'error_types': dict(self.error_types), 'last_error_ago': time.time() - self.last_error_time if self.last_error_time > 0 else None, 'memory_usage_mb': memory_mb}

    def cleanup_memory(self): 
        try: gc.collect()
        except: pass

# ====================== PORTFOLIO ======================
class Portfolio:
    def __init__(self): self._positions, self._lock = {}, RLock()
    def add_position(self, position: Position):
        if not isinstance(position, Position): raise ValueError("Invalid position")
        position.__post_init__()
        with self._lock: self._positions[position.symbol] = position
    def remove_position(self, symbol: str) -> Optional[Position]:
        with self._lock: return self._positions.pop(symbol, None) if symbol and isinstance(symbol, str) else None
    def get_position(self, symbol: str) -> Optional[Position]:
        with self._lock: return self._positions.get(symbol) if symbol and isinstance(symbol, str) else None
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
                if not all(isinstance(x, str) and x.strip() for x in [symbol, side, mode, tag]) or side not in ["long", "short"]: raise ValueError("Invalid parameters")
                if not all(isinstance(x, D) and x.is_finite() for x in [quantity, entry_price, leverage]) or quantity <= 0 or entry_price <= 0 or not 0 < leverage <= 100: raise ValueError("Invalid numeric parameters")
                if stop_loss > 0:
                    if (side == "long" and stop_loss >= entry_price) or (side == "short" and stop_loss <= entry_price): raise ValueError("Invalid stop loss")
                if take_profit > 0:
                    if (side == "long" and take_profit <= entry_price) or (side == "short" and take_profit >= entry_price): raise ValueError("Invalid take profit")
                if not self.exchange.is_market_active(symbol): raise ExchangeUnavailable(f"Market {symbol} not active")
                tick_size, step_size = self.exchange.get_step_sizes(symbol)
                min_notional = self.exchange.get_min_notional(symbol)
                rounded_qty, rounded_entry = quantize_step(quantity, step_size), quantize_step(entry_price, tick_size)
                rounded_sl, rounded_tp = quantize_step(stop_loss, tick_size) if stop_loss > 0 else D("0"), quantize_step(take_profit, tick_size) if take_profit > 0 else D("0")
                if rounded_qty <= 0: raise ValueError(f"Quantity too small: {quantity}")
                if rounded_qty * rounded_entry < min_notional: raise InsufficientFundsError(f"Notional below minimum")
                position = Position(symbol=symbol, side=side, qty=rounded_qty, entry=rounded_entry, sl=rounded_sl, tp=rounded_tp, lev=leverage, opened_at=time.time(), mode=mode, tag=tag)
                if self.mode == "paper":
                    if not self.db_manager.save_position(position): raise Exception("Failed to save paper position")
                    self.notifier.send(f"📝 PAPER: Opened {side.upper()} {symbol} qty={rounded_qty} @ {rounded_entry}")
                else:
                    order_side, client_order_id = "buy" if side == "long" else "sell", f"omega_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
                    try:
                        if hasattr(self.exchange.exchange, 'set_leverage'): self.exchange.exchange.set_leverage(int(leverage), symbol)
                    except Exception as e: self.logger.warning(f"Set leverage failed {symbol}: {e}")
                    order = self.exchange.create_order(symbol=symbol, order_type="market", side=order_side, amount=rounded_qty, params={"newClientOrderId": client_order_id})
                    position.entry_order_id, position.client_order_id = order.get("id"), client_order_id
                    if order.get("average"): position.entry = safe_decimal(order["average"])
                    elif order.get("price"): position.entry = safe_decimal(order["price"])
                    self.db_manager.save_position(position)
                    self.notifier.send(f"✅ LIVE: Opened {side.upper()} {symbol} qty={position.qty} @ {position.entry}")
                return position
            except Exception as e: self.logger.error(f"Open position failed {symbol}: {e}"); self.notifier.send(f"❌ Failed to open {symbol}: {str(e)[:100]}"); return None

    def close_position(self, position: Position, exit_price: D, reason: str = "MANUAL") -> D:
        with self._execution_lock:
            try:
                if not isinstance(position, Position) or not isinstance(exit_price, D) or exit_price <= 0 or not exit_price.is_finite(): raise ValueError("Invalid parameters")
                pnl = position.unrealized_pnl(exit_price)
                if self.mode == "live":
                    try:
                        if position.sl_order_id: self.exchange.cancel_order(position.sl_order_id, position.symbol)
                        if position.tp_order_id: self.exchange.cancel_order(position.tp_order_id, position.symbol)
                        opposite_side = "sell" if position.side == "long" else "buy"
                        self.exchange.create_order(symbol=position.symbol, order_type="market", side=opposite_side, amount=position.qty, params={"reduceOnly": True})
                    except Exception as e: self.logger.error(f"Live close failed {position.symbol}: {e}")
                self.db_manager.delete_position(position.symbol)
                pnl_emoji = "💚" if pnl >= 0 else "❤️"
                self.notifier.send(f"{pnl_emoji} Closed {position.side.upper()} {position.symbol} @ {exit_price} PnL: ${pnl:.2f} ({reason})")
                return pnl
            except Exception as e: self.logger.error(f"Close position failed {position.symbol}: {e}"); return D("0")

    def update_stop_loss(self, position: Position, new_sl: D):
        with self._execution_lock:
            try:
                if not isinstance(position, Position) or not isinstance(new_sl, D) or not new_sl.is_finite(): raise ValueError("Invalid parameters")
                if new_sl > 0:
                    if (position.side == "long" and new_sl >= position.entry) or (position.side == "short" and new_sl <= position.entry): raise ValueError("Invalid stop loss logic")
                tick_size, _ = self.exchange.get_step_sizes(position.symbol)
                rounded_sl = quantize_step(new_sl, tick_size) if new_sl > 0 else D("0")
                old_sl = position.sl; position.sl = rounded_sl
                if not self.db_manager.save_position(position): position.sl = old_sl; raise Exception("Database save failed")
                if self.mode == "live" and position.sl_order_id:
                    try:
                        self.exchange.cancel_order(position.sl_order_id, position.symbol); position.sl_order_id = None
                        if rounded_sl > 0:
                            opposite_side = "sell" if position.side == "long" else "buy"
                            sl_order = self.exchange.create_order(symbol=position.symbol, order_type="STOP_MARKET", side=opposite_side, amount=position.qty, params={"stopPrice": float(rounded_sl), "reduceOnly": True})
                            position.sl_order_id = sl_order.get("id"); self.db_manager.save_position(position)
                    except Exception as e: self.logger.error(f"Update live SL failed {position.symbol}: {e}")
                self.notifier.send(f"🔧 Updated SL for {position.symbol}: {old_sl} → {rounded_sl}")
            except Exception as e: self.logger.error(f"Update SL failed {position.symbol}: {e}")

# ====================== MAIN BOT ======================
class OmegaXBot:
    def __init__(self):
                self.logger = logging.getLogger(self.__class__.__name__)
        self.mode = MODE
        try:
            self.notifier = Notifier(TELEGRAM_ENABLED)
            self.health_monitor = HealthMonitor()
            self.exchange = ExchangeWrapper(self.mode)
            self.db_manager = DatabaseManager(DB_PATH)
            self.strategy = Strategy()
            self.symbols = self._validate_symbols(SYMBOLS)
            if not self.symbols: raise ConfigurationError("No valid symbols")
            self.data_manager = DataManager(self.exchange, self.symbols)
            self.indicator_cache = IndicatorCache()
            self.execution_engine = ExecutionEngine(self.exchange, self.db_manager, self.notifier, self.mode)
            self.portfolio = Portfolio()
            self.risk_manager = RiskManager(INITIAL_BALANCE)
            self._shutdown_event = ThreadEvent()
            self._running = False
            self._last_error_notification = 0.0
            self._last_health_check = 0.0
            self._restore_state()
            self.notifier.send_critical(f"🚀 OmegaX Bot initialized\nMode: {self.mode.upper()}\nSymbols: {len(self.symbols)}\nBalance: ${self.risk_manager.balance}")
        except Exception as e:
            self.logger.error(f"Bot init failed: {e}")
            if hasattr(self, 'notifier'): self.notifier.send_critical(f"💥 Init failed: {e}")
            raise

    def _validate_symbols(self, symbols: List[str]) -> List[str]:
        valid_symbols = []
        for symbol in symbols:
            try:
                if symbol in self.exchange.exchange.markets:
                    market = self.exchange.exchange.markets[symbol]
                    if market.get('active', True) and market.get('type') == 'future': valid_symbols.append(symbol)
                    else: self.logger.warning(f"Symbol {symbol} not active futures market")
                else: self.logger.warning(f"Symbol {symbol} not found")
            except Exception as e: self.logger.error(f"Error validating {symbol}: {e}")
        if "BTC/USDT" not in valid_symbols and "BTC/USDT" in self.exchange.exchange.markets: valid_symbols.append("BTC/USDT")
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
                        if day_anchor < current_midnight:
                            self.risk_manager.day_anchor, self.risk_manager.daily_pnl, self.risk_manager.consec_losses = current_midnight, D("0"), 0
                        else: self.risk_manager.day_anchor = day_anchor
                except Exception as e: self.logger.error(f"Risk state restore failed: {e}")
            positions = self.db_manager.load_positions()
            for position in positions.values():
                try:
                    if position.symbol in self.symbols:
                        if position.is_expired(): self.db_manager.delete_position(position.symbol)
                        else: self.portfolio.add_position(position)
                    else: self.db_manager.delete_position(position.symbol)
                except Exception as e: self.logger.error(f"Position restore failed {position.symbol}: {e}")
        except Exception as e: self.logger.error(f"State restoration failed: {e}"); self.health_monitor.record_error(e, "state_restoration")

    def _get_current_prices(self) -> Dict[str, D]:
        prices = {}
        for symbol in self.symbols:
            try:
                price = self.data_manager.get_latest_price(symbol)
                if price is None:
                    ticker = self.exchange.fetch_ticker(symbol)
                    price_raw = ticker.get('last') or ticker.get('close') or ticker.get('ask') or ticker.get('bid')
                    price = safe_decimal(price_raw)
                if price and price > 0: prices[symbol] = price
            except Exception as e:
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
            try:
                atr_series = AverageTrueRange(df["high"], df["low"], df["close"], 14).average_true_range()
                if atr_series.empty or atr_series.isna().all(): return
                atr_value = atr_series.iloc[-1]
                if np.isnan(atr_value) or atr_value <= 0: return
                atr_decimal = safe_decimal(atr_value)
            except: return
            unrealized_pnl, risk_amount = position.unrealized_pnl(current_price), position.risk_amount()
            if risk_amount <= 0: return
            r_multiple = unrealized_pnl / risk_amount
            if r_multiple >= D("3"): trail_mult = D("0.7")
            elif r_multiple >= D("2"): trail_mult = D("1.0")
            elif r_multiple >= D("1"): trail_mult = D("1.4")
            else: return
            if position.side == "long":
                new_sl = current_price - (atr_decimal * trail_mult)
                if new_sl > position.sl: self.execution_engine.update_stop_loss(position, new_sl)
            else:
                new_sl = current_price + (atr_decimal * trail_mult)
                if new_sl < position.sl: self.execution_engine.update_stop_loss(position, new_sl)
        except Exception as e: self.logger.error(f"Trailing stop failed {position.symbol}: {e}"); self.health_monitor.record_error(e, "trailing_stop")

    def _manage_positions(self, prices: Dict[str, D]):
        positions_to_close = []
        for position in self.portfolio.get_all_positions():
            try:
                current_price = prices.get(position.symbol)
                if not current_price or current_price <= 0: continue
                if position.is_expired(): positions_to_close.append((position, current_price, "TIME_LIMIT")); continue
                if position.sl > 0:
                    if ((position.side == "long" and current_price <= position.sl) or (position.side == "short" and current_price >= position.sl)):
                        positions_to_close.append((position, current_price, "STOP_LOSS")); continue
                if position.tp > 0:
                    if ((position.side == "long" and current_price >= position.tp) or (position.side == "short" and current_price <= position.tp)):
                        positions_to_close.append((position, current_price, "TAKE_PROFIT")); continue
                if not position.is_hedge: self._update_trailing_stop(position, current_price)
            except Exception as e: self.logger.error(f"Position management error {position.symbol}: {e}"); self.health_monitor.record_error(e, "position_management")
        for position, price, reason in positions_to_close:
            try:
                removed_position = self.portfolio.remove_position(position.symbol)
                if removed_position:
                    pnl = self.execution_engine.close_position(removed_position, price, reason)
                    new_balance = self.risk_manager.balance + pnl
                    self.risk_manager.update_balance(new_balance)
                    self.db_manager.save_risk_state(self.risk_manager)
            except Exception as e: self.logger.error(f"Close position failed {position.symbol}: {e}"); self.health_monitor.record_error(e, "position_closing")

    def _analyze_symbol(self, symbol: str, df5: pd.DataFrame, df15: pd.DataFrame, df1h: pd.DataFrame, current_price: D) -> Optional[Tuple[str, D, D, D, D]]:
        try:
            if not all(validate_dataframe(df, 20) for df in [df5, df15, df1h]) or not isinstance(current_price, D) or current_price <= 0: return None
            if not all(self.data_manager.is_data_fresh(symbol, tf) for tf in ["5m", "15m", "1h"]): return None
            indicators = self.indicator_cache.get_indicators(symbol, df5, df15, df1h)
            if not indicators: return None
            regime, vol_spike = self.strategy.detect_regime(df5, df15)
            btc_df5 = self.data_manager.get_data("BTC/USDT", "5m")
            cs_rank_map = self.strategy.cross_sectional_mom_rank({symbol: df1h})
            cs_rank = cs_rank_map.get(symbol, 0.0)
            alpha_scores = self.strategy.alpha_scores(symbol, df5, df15, df1h, btc_df5, cs_rank, 0.0)
            if not alpha_scores: return None
            try:
                long_scores = [scores[0] for scores in alpha_scores.values() if np.isfinite(scores[0])]
                short_scores = [scores[1] for scores in alpha_scores.values() if np.isfinite(scores[1])]
                if not long_scores or not short_scores: return None
                long_score, short_score = sum(long_scores) / len(long_scores), sum(short_scores) / len(short_scores)
            except: return None
            threshold = 0.6 + (0.2 if vol_spike else 0.0)
            side, confidence = None, D("0")
            if long_score > threshold and long_score > short_score: side, confidence = "long", safe_decimal(min(max(0.0, long_score), 1.0))
            elif short_score > threshold and short_score > long_score: side, confidence = "short", safe_decimal(min(max(0.0, short_score), 1.0))
            if not side or confidence <= 0: return None
            atr_series = indicators.get('atr')
            if atr_series is None or atr_series.empty: return None
            try:
                current_atr = atr_series.iloc[-1]
                if np.isnan(current_atr) or current_atr <= 0: return None
                atr_decimal = safe_decimal(current_atr)
            except: return None
            sl_mult, tp_mult = (D("1.5"), D("2.5")) if "Trending" in regime else (D("1.2"), D("1.8"))
            if vol_spike: sl_mult *= D("1.2"); tp_mult *= D("0.8")
            if side == "long": stop_loss, take_profit = current_price - (atr_decimal * sl_mult), current_price + (atr_decimal * tp_mult)
            else: stop_loss, take_profit = current_price + (atr_decimal * sl_mult), current_price - (atr_decimal * tp_mult)
            if stop_loss <= 0 or take_profit <= 0: return None
            risk, reward = abs(current_price - stop_loss), abs(take_profit - current_price)
            if risk <= 0 or reward / risk < D("1.2"): return None
            return side, current_price, stop_loss, take_profit, confidence
        except Exception as e: self.logger.error(f"Analysis failed {symbol}: {e}"); self.health_monitor.record_error(e, "symbol_analysis"); return None

    def _scan_for_opportunities(self, prices: Dict[str, D]):
        if self.risk_manager.risk_off: return
        open_positions = len(self.portfolio.get_open_positions())
        if open_positions >= MAX_CONCURRENT_POS: return
        total_risk = sum(position.risk_amount() for position in self.portfolio.get_all_positions() if not position.is_hedge)
        if total_risk / self.risk_manager.balance >= MAX_TOTAL_RISK: return
        opportunities_found, max_new_positions = 0, min(3, MAX_CONCURRENT_POS - open_positions)
        for symbol in self.symbols:
            try:
                if opportunities_found >= max_new_positions: break
                if self.portfolio.get_position(symbol) or not self.risk_manager.can_trade(symbol): continue
                current_price = prices.get(symbol)
                if not current_price: continue
                df5, df15, df1h = self.data_manager.get_data(symbol, "5m"), self.data_manager.get_data(symbol, "15m"), self.data_manager.get_data(symbol, "1h")
                if not all(validate_dataframe(df) for df in [df5, df15, df1h]): continue
                signal = self._analyze_symbol(symbol, df5, df15, df1h, current_price)
                if not signal: continue
                side, entry_price, stop_loss, take_profit, confidence = signal
                base_risk = RISK_PER_TRADE * confidence
                position_size = self.risk_manager.position_size(symbol, entry_price, stop_loss, base_risk)
                if position_size <= 0: continue
                min_notional = self.exchange.get_min_notional(symbol)
                if position_size * entry_price < min_notional: continue
                new_risk = abs(entry_price - stop_loss) * position_size
                if (total_risk + new_risk) / self.risk_manager.balance > MAX_TOTAL_RISK: continue
                position = self.execution_engine.open_position(symbol=symbol, side=side, quantity=position_size, entry_price=entry_price, stop_loss=stop_loss, take_profit=take_profit, leverage=D("10"), mode="directional", tag="strategy")
                if position: self.portfolio.add_position(position); self.risk_manager.note_trade_opened(symbol); opportunities_found += 1; total_risk += new_risk
            except Exception as e: self.logger.error(f"Scanning error {symbol}: {e}"); self.health_monitor.record_error(e, "opportunity_scanning")

    def _periodic_tasks(self):
        try:
                        self.db_manager.save_risk_state(self.risk_manager)
            self.health_monitor.cleanup_memory()
            cache_stats = self.indicator_cache._cleanup_cache() if len(self.indicator_cache.cache) > 800 else None
            self._send_periodic_report()
            if random.random() < 0.1: self.db_manager.create_backup(f"{DB_PATH}.backup.{int(time.time())}")
        except Exception as e: self.logger.error(f"Periodic tasks failed: {e}"); self.health_monitor.record_error(e, "periodic_tasks")

    def _send_periodic_report(self):
        try:
            prices = self._get_current_prices()
            portfolio_summary = {'total_positions': len(self.portfolio.get_all_positions()), 'long_positions': sum(1 for p in self.portfolio.get_all_positions() if p.side == "long"), 'short_positions': sum(1 for p in self.portfolio.get_all_positions() if p.side == "short"), 'total_exposure': float(sum(p.qty * prices.get(p.symbol, p.entry) for p in self.portfolio.get_all_positions() if not p.is_hedge)), 'total_unrealized_pnl': float(sum(p.unrealized_pnl(prices.get(p.symbol, p.entry)) for p in self.portfolio.get_all_positions()))}
            risk_status = {'balance': float(self.risk_manager.balance), 'drawdown': float(self.risk_manager.drawdown), 'daily_pnl': float(self.risk_manager.daily_pnl), 'risk_off': self.risk_manager.risk_off, 'consec_losses': self.risk_manager.consec_losses}
            health_status = self.health_monitor.get_health_status()
            total_return = ((risk_status['balance'] - float(INITIAL_BALANCE)) / float(INITIAL_BALANCE)) * 100
            report = f"📊 <b>OmegaX Status</b>\n💰 Balance: ${risk_status['balance']:.2f}\nReturn: {total_return:+.2f}%\nDD: {risk_status['drawdown']:.2%}\nDaily: ${risk_status['daily_pnl']:+.2f}\n📈 Positions: {portfolio_summary['total_positions']} (L:{portfolio_summary['long_positions']} S:{portfolio_summary['short_positions']})\nExposure: ${portfolio_summary['total_exposure']:.2f}\nPnL: ${portfolio_summary['total_unrealized_pnl']:+.2f}\n⚠️ Risk Off: {'Yes' if risk_status['risk_off'] else 'No'}\n🔧 Status: {health_status['status'].title()}\nUptime: {health_status['uptime_seconds']/3600:.1f}h\nErrors: {health_status['error_count']}"
            self.notifier.send(report)
        except Exception as e: self.logger.error(f"Periodic report failed: {e}")

    def _health_check(self):
        if time.time() - self._last_health_check < 300: return
        try:
            try: self.exchange.fetch_ticker("BTC/USDT")
            except Exception as e: self.health_monitor.record_error(e, "exchange_connectivity")
            try: self.db_manager.load_risk_state()
            except Exception as e: self.health_monitor.record_error(e, "database_connectivity")
            fresh_count = sum(1 for s in self.symbols if any(self.data_manager.is_data_fresh(s, tf) for tf in ["5m", "15m", "1h"]))
            if fresh_count / len(self.symbols) < 0.8: self.health_monitor.record_error(Exception(f"Data freshness low: {fresh_count}/{len(self.symbols)}"), "data_freshness")
            if self.risk_manager.drawdown > D("0.06"): self.logger.warning(f"High drawdown: {float(self.risk_manager.drawdown):.2%}")
            self._last_health_check = time.time()
        except Exception as e: self.logger.error(f"Health check failed: {e}"); self.health_monitor.record_error(e, "health_check")

    def run(self):
        self._running = True
        self.logger.info("Starting OmegaX trading bot...")
        def signal_handler(signum, frame): self.logger.info(f"Signal {signum} received, shutting down..."); self.stop()
        try: signal.signal(signal.SIGINT, signal_handler); signal.signal(signal.SIGTERM, signal_handler)
        except: pass
        last_data_update = last_periodic_task = 0.0
        loop_count = 0
        try:
            self.logger.info("Initial data load...")
            for symbol in self.symbols:
                for tf in ["5m", "15m", "1h"]: self.data_manager.update_data(symbol, tf)
            while self._running and not self._shutdown_event.is_set():
                loop_start = time.time(); loop_count += 1
                try:
                    self._health_check()
                    if time.time() - last_data_update > 60:
                        for symbol in self.symbols:
                            for tf in ["5m", "15m", "1h"]: self.data_manager.update_data(symbol, tf)
                        last_data_update = time.time()
                    prices = self._get_current_prices()
                    if not prices: time.sleep(5); continue
                    self._manage_positions(prices)
                    self._scan_for_opportunities(prices)
                    if time.time() - last_periodic_task > 900: self._periodic_tasks(); last_periodic_task = time.time()
                    if loop_count % 100 == 0: self.logger.info(f"Loop {loop_count}: Positions: {len(self.portfolio.get_all_positions())}, Balance: ${self.risk_manager.balance}")
                except HardStopTriggered as e:
                    self.logger.critical(f"Hard stop: {e}"); self.notifier.send_critical(f"🛑 HARD STOP: {e}")
                    emergency_prices = self._get_current_prices()
                    for position in self.portfolio.get_all_positions():
                        try:
                            price = emergency_prices.get(position.symbol, position.entry)
                            pnl = self.execution_engine.close_position(position, price, "EMERGENCY_HARD_STOP")
                            self.portfolio.remove_position(position.symbol)
                            self.risk_manager.balance += pnl
                        except Exception as e: self.logger.error(f"Emergency close failed {position.symbol}: {e}")
                    self.db_manager.save_risk_state(self.risk_manager)
                    with self.risk_manager._lock: self.risk_manager.paused_until = time.time() + 3600
                    self.notifier.send_critical("🔒 All positions closed. Trading paused 1 hour."); break
                except ExchangeUnavailable as e:
                    self.logger.error(f"Exchange unavailable: {e}"); self.health_monitor.record_error(e, "exchange_unavailable")
                    with self.risk_manager._lock: self.risk_manager._risk_off_until = max(self.risk_manager._risk_off_until, time.time() + 900)
                    if time.time() - self._last_error_notification > 300: self.notifier.send(f"⚠️ Exchange unavailable: {str(e)[:100]}"); self._last_error_notification = time.time()
                    time.sleep(30); continue
                except Exception as e:
                    self.logger.error(f"Main loop error: {e}"); self.health_monitor.record_error(e, "main_loop")
                    if time.time() - self._last_error_notification > 300: self.notifier.send(f"⚠️ Loop error: {str(e)[:100]}"); self._last_error_notification = time.time()
                loop_time = time.time() - loop_start
                base_sleep = 8.0 if loop_time > 3.0 else (3.0 if loop_time < 1.0 else 5.0)
                sleep_time = max(0.5, base_sleep - loop_time + random.uniform(-0.5, 0.5))
                if self._shutdown_event.wait(sleep_time): break
        except KeyboardInterrupt: self.logger.info("Keyboard interrupt"); self.stop()
        except Exception as e: self.logger.critical(f"Fatal error: {e}"); self.health_monitor.record_error(e, "fatal_error"); self.notifier.send_critical(f"💥 FATAL: {str(e)[:200]}"); raise
        finally: self._cleanup()

    def stop(self): self.logger.info("Stopping bot..."); self._running = False; self._shutdown_event.set()

    def _cleanup(self):
        try:
            self.logger.info("Cleanup starting...")
            try: self.db_manager.save_risk_state(self.risk_manager)
            except Exception as e: self.logger.error(f"Risk state save failed: {e}")
            try:
                for position in self.portfolio.get_all_positions(): self.db_manager.save_position(position)
            except Exception as e: self.logger.error(f"Position save failed: {e}")
            try:
                backup_path = f"{DB_PATH}.backup.{int(time.time())}"
                if self.db_manager.create_backup(backup_path): self.logger.info(f"Backup created: {backup_path}")
            except Exception as e: self.logger.error(f"Backup failed: {e}")
            try:
                final_status = {'balance': float(self.risk_manager.balance), 'positions': len(self.portfolio.get_all_positions()), 'uptime': time.time() - self.health_monitor.start_time, 'errors': self.health_monitor.error_count}
                self.notifier.send_critical(f"🔄 Shutdown complete\nBalance: ${final_status['balance']:.2f}\nPositions: {final_status['positions']}\nUptime: {final_status['uptime']/3600:.1f}h\nErrors: {final_status['errors']}")
            except Exception as e: self.logger.error(f"Final status failed: {e}")
            try: self.health_monitor.cleanup_memory(); self.indicator_cache._cleanup_cache()
            except: pass
            self.logger.info("Cleanup completed")
        except Exception as e: self.logger.error(f"Cleanup failed: {e}")

# ====================== FLASK HEALTH SERVER ======================
def create_health_server(bot: OmegaXBot) -> Flask:
    app = Flask(__name__)
    app.logger.disabled = True
    @app.route('/', methods=["GET", "HEAD"])
    def index(): return "OmegaX Bot running", 200
    @app.route('/ping', methods=["GET", "HEAD"])
    def ping(): return "pong", 200
    @app.route('/health', methods=["GET", "HEAD"])
    def health():
        try:
            prices = bot._get_current_prices()
            health_data = {'ok': True, 'mode': bot.mode, 'balance': str(bot.risk_manager.balance), 'drawdown': str(bot.risk_manager.drawdown), 'positions': len(bot.portfolio.get_all_positions()), 'risk_off': bot.risk_manager.risk_off, 'prices_available': len(prices), 'uptime': time.time() - bot.health_monitor.start_time}
            return jsonify(health_data), 200
        except Exception as e: return jsonify({'error': str(e)}), 500
    @app.route('/metrics', methods=["GET"])
    def metrics():
        try: return jsonify({'balance': str(bot.risk_manager.balance), 'drawdown': str(bot.risk_manager.drawdown), 'daily_pnl': str(bot.risk_manager.daily_pnl), 'open_positions': len(bot.portfolio.get_all_positions()), 'symbols': len(bot.symbols)}), 200
        except Exception as e: return jsonify({'error': str(e)}), 500
    @app.route('/api/status', methods=["GET"])
    def status(): return jsonify({'status': 'running', 'mode': bot.mode}), 200
    @app.route('/api/positions', methods=["GET"])
    def positions():
        try:
            pos_data = [{'symbol': p.symbol, 'side': p.side, 'qty': str(p.qty), 'entry': str(p.entry), 'sl': str(p.sl), 'tp': str(p.tp), 'opened_at': p.opened_at} for p in bot.portfolio.get_all_positions()]
            return jsonify(pos_data), 200
        except Exception as e: return jsonify({'error': str(e)}), 500
    return app

# ====================== ENTRY POINT ======================
def main():
    setup_logging()
    logger = logging.getLogger("main")
    try:
        logger.info(f"Starting OmegaX Bot - Mode: {MODE}, Symbols: {len(SYMBOLS)}, Balance: ${INITIAL_BALANCE}")
        bot = OmegaXBot()
        # Start health server in background
        app = create_health_server(bot)
        server_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False), daemon=True)
        server_thread.start()
        logger.info(f"Health server started on port {PORT}")
        bot.run()
    except KeyboardInterrupt: logger.info("Bot stopped by user")
    except ConfigurationError as e: logger.critical(f"Config error: {e}"); return 1
    except Exception as e: logger.critical(f"Bot crashed: {e}"); return 1
    logger.info("Bot shutdown complete")
    return 0

if __name__ == "__main__": exit(main())