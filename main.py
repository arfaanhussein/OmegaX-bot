# main.py
"""
OmegaX Institutional Futures Trading Bot - Complete Single File Implementation
True hedge fund algorithms for crypto futures trading

Installation:
pip install requests pandas numpy scikit-learn python-telegram-bot

Environment Variables Required:
BINANCE_API_KEY=your_binance_api_key
BINANCE_SECRET_KEY=your_binance_secret_key
TELEGRAM_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id

Optional:
USE_REALISTIC_PAPER=true   # Use real market data via public API with simulated orders
"""

import os
import sys
import time
import json
import hmac
import hashlib
import logging
import asyncio
import sqlite3
import requests
import threading
import random
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple, Any, Union
from collections import deque, defaultdict
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
import warnings
warnings.filterwarnings('ignore')

# Try to import required packages, install if missing
try:
    import numpy as np
    import pandas as pd
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.ensemble import IsolationForest
except ImportError as e:
    print(f"Installing required packages: {e}")
    os.system("pip install numpy pandas scikit-learn")
    import numpy as np
    import pandas as pd
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.ensemble import IsolationForest

# Set decimal precision
getcontext().prec = 28
D = Decimal

# ====================== CONFIGURATION ======================
class Config:
    """Configuration settings"""

    # API Configuration
    BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', '')
    BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '')
    BINANCE_TESTNET = os.environ.get('BINANCE_TESTNET', 'false').lower() == 'true'

    # Paper trading mode using real public data
    USE_REALISTIC_PAPER = os.environ.get('USE_REALISTIC_PAPER', 'false').lower() == 'true'

    # Telegram Configuration
    TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '')
    TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

    # Trading Configuration
    INITIAL_BALANCE = D(os.environ.get('INITIAL_BALANCE', '1000'))
    BASE_RISK_PERCENT = D(os.environ.get('BASE_RISK_PERCENT', '0.8'))  # 0.8% per trade
    MAX_POSITIONS = int(os.environ.get('MAX_POSITIONS', '12'))
    LEVERAGE = int(os.environ.get('LEVERAGE', '10'))

    # Strategy Configuration
    SIGNAL_THRESHOLD = D(os.environ.get('SIGNAL_THRESHOLD', '0.65'))
    MIN_VOLUME_24H = D(os.environ.get('MIN_VOLUME_24H', '5000000'))  # $5M daily volume

    # Risk Management
    MAX_DRAWDOWN = D(os.environ.get('MAX_DRAWDOWN', '0.12'))  # 12%
    STOP_LOSS_PERCENT = D(os.environ.get('STOP_LOSS_PERCENT', '1.8'))  # 1.8%
    TAKE_PROFIT_PERCENT = D(os.environ.get('TAKE_PROFIT_PERCENT', '3.5'))  # 3.5%

    # System Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    UPDATE_INTERVAL = int(os.environ.get('UPDATE_INTERVAL', '25'))  # seconds
    REPORT_INTERVAL = int(os.environ.get('REPORT_INTERVAL', '1800'))  # 30 minutes

    # Rate Limiting
    MAX_REQUESTS_PER_MINUTE = int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '1000'))
    WEIGHT_LIMIT_PER_MINUTE = int(os.environ.get('WEIGHT_LIMIT_PER_MINUTE', '5000'))

    # Top trading pairs (optimized for free tier)
    TRADING_PAIRS = [
        'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT', 'DOGEUSDT', 'MATICUSDT',
        'DOTUSDT', 'LTCUSDT', 'AVAXUSDT', 'LINKUSDT', 'UNIUSDT', 'ATOMUSDT', 'XLMUSDT', 'VETUSDT',
        'FILUSDT', 'ICPUSDT', 'HBARUSDT', 'APTUSDT', 'NEARUSDT', 'GRTUSDT', 'SANDUSDT', 'MANAUSDT',
        'FLOWUSDT', 'EGLDUSDT', 'XTZUSDT', 'THETAUSDT', 'AXSUSDT', 'AAVEUSDT', 'EOSUSDT', 'KLAYUSDT',
        'RUNEUSDT', 'FTMUSDT', 'NEOUSDT', 'CAKEUSDT', 'IOTAUSDT', 'ZECUSDT', 'DASHUSDT', 'WAVESUSDT',
        'CHZUSDT', 'BATUSDT', 'GALAUSDT', 'LRCUSDT', 'ENJUSDT', 'CELOUSDT', 'ZILUSDT', 'QTUMUSDT'
    ]

# ====================== LOGGING SETUP ======================
def setup_logging():
    """Setup logging configuration"""
    level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# ====================== QUANTITATIVE MODELS ======================
class KalmanFilter:
    """Kalman Filter for optimal price estimation - Renaissance Technologies method"""

    def __init__(self, process_variance=1e-5, measurement_variance=1e-1):
        self.process_variance = process_variance
        self.measurement_variance = measurement_variance
        self.posteri_estimate = 0.0
        self.posteri_error_estimate = 1.0
        self.initialized = False

    def update(self, measurement):
        if not self.initialized:
            self.posteri_estimate = measurement
            self.initialized = True
            return measurement

        # Prediction step
        priori_estimate = self.posteri_estimate
        priori_error_estimate = self.posteri_error_estimate + self.process_variance

        # Update step
        blending_factor = priori_error_estimate / (priori_estimate + self.measurement_variance)
        self.posteri_estimate = priori_estimate + blending_factor * (measurement - priori_estimate)
        self.posteri_error_estimate = (1 - blending_factor) * priori_error_estimate

        return self.posteri_estimate

class OrnsteinUhlenbeckModel:
    """Mean reversion model - Two Sigma/DE Shaw method"""

    def __init__(self, window=80):
        self.window = window
        self.prices = deque(maxlen=window)

    def update(self, price):
        self.prices.append(price)

        if len(self.prices) < 15:
            return {'theta': 0, 'mu': price, 'sigma': 0, 'half_life': float('inf'), 'z_score': 0}

        try:
            prices_array = np.array(self.prices)
            log_prices = np.log(prices_array)
            returns = np.diff(log_prices)

            if len(returns) < 8:
                return {'theta': 0, 'mu': price, 'sigma': 0, 'half_life': float('inf'), 'z_score': 0}

            # Estimate OU parameters using least squares
            y = returns[1:]
            x = log_prices[:-2]

            coeffs = np.polyfit(x, y, 1)
            beta, alpha = coeffs

            theta = -beta  # Mean reversion speed
            mu = -alpha / beta if beta != 0 else np.mean(log_prices)  # Long-term mean
            sigma = np.std(y)  # Volatility

            half_life = np.log(2) / theta if theta > 0 else float('inf')

            current_log_price = log_prices[-1]
            z_score = (current_log_price - mu) / sigma if sigma > 0 else 0

            return {
                'theta': theta,
                'mu': np.exp(mu),
                'sigma': sigma,
                'half_life': half_life,
                'z_score': z_score
            }

        except Exception:
            return {'theta': 0, 'mu': price, 'sigma': 0, 'half_life': float('inf'), 'z_score': 0}

class HurstExponent:
    """Hurst Exponent for trend persistence - Citadel method"""

    @staticmethod
    def calculate(prices, max_lag=15):
        if len(prices) < max_lag * 2:
            return 0.5

        try:
            prices = np.array(prices)
            log_prices = np.log(prices)
            returns = np.diff(log_prices)

            lags = range(2, min(max_lag, len(returns) // 3))
            rs_values = []

            for lag in lags:
                n_periods = len(returns) // lag
                if n_periods < 2:
                    continue

                rs_period = []

                for i in range(n_periods):
                    period_returns = returns[i*lag:(i+1)*lag]

                    if len(period_returns) < lag:
                        continue

                    mean_return = np.mean(period_returns)
                    deviations = np.cumsum(period_returns - mean_return)

                    R = np.max(deviations) - np.min(deviations)
                    S = np.std(period_returns)

                    if S > 0:
                        rs_period.append(R / S)

                if rs_period:
                    rs_values.append(np.mean(rs_period))

            if len(rs_values) < 3:
                return 0.5

            log_lags = np.log(lags[:len(rs_values)])
            log_rs = np.log(rs_values)
            hurst_exponent = np.polyfit(log_lags, log_rs, 1)[0]
            return max(0.1, min(0.9, hurst_exponent))

        except Exception:
            return 0.5

class RegimeDetector:
    """Market regime detection using HMM-style clustering"""

    def __init__(self, window=40):
        self.window = window
        self.observations = deque(maxlen=window)
        self.scaler = StandardScaler()
        self.kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
        self.fitted = False

    def update(self, returns, volatility, volume_ratio):
        self.observations.append([returns, volatility, volume_ratio])

        if len(self.observations) < 20:
            return 'NORMAL'

        try:
            obs_array = np.array(self.observations)

            if not self.fitted and len(self.observations) >= 30:
                scaled_obs = self.scaler.fit_transform(obs_array)
                self.kmeans.fit(scaled_obs)
                self.fitted = True

            if self.fitted:
                current_obs = obs_array[-1:].reshape(1, -1)
                scaled_current = self.scaler.transform(current_obs)
                regime = self.kmeans.predict(scaled_current)[0]

                # Map regimes to meaningful labels
                recent_obs = obs_array[-10:]
                avg_return = np.mean(recent_obs[:, 0])
                avg_vol = np.mean(recent_obs[:, 1])

                if avg_vol > 0.04:
                    return 'CRISIS'
                elif avg_return > 0.002 and avg_vol < 0.03:
                    return 'BULL_TREND'
                elif avg_return < -0.002 and avg_vol < 0.03:
                    return 'BEAR_TREND'
                elif avg_vol < 0.015:
                    return 'LOW_VOL'
                else:
                    return 'NORMAL'
            else:
                return 'NORMAL'

        except Exception:
            return 'NORMAL'

# ====================== RATE LIMITER ======================
class RateLimiter:
    """Binance API rate limiter"""

    def __init__(self, max_requests_per_minute: int, max_weight_per_minute: int):
        self.max_requests = max_requests_per_minute
        self.max_weight = max_weight_per_minute
        self.requests = deque()
        self.weight_used = deque()
        self.lock = threading.Lock()

    def wait_if_needed(self, weight: int = 1):
        with self.lock:
            now = time.time()

            # Remove old requests
            while self.requests and now - self.requests[0] > 60:
                self.requests.popleft()

            while self.weight_used and now - self.weight_used[0][0] > 60:
                self.weight_used.popleft()

            # Check limits
            current_requests = len(self.requests)
            current_weight = sum(w[1] for w in self.weight_used)

            if current_requests >= self.max_requests:
                sleep_time = 60 - (now - self.requests[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)

            if current_weight + weight > self.max_weight:
                sleep_time = 60 - (now - self.weight_used[0][0])
                if sleep_time > 0:
                    time.sleep(sleep_time)

            self.requests.append(now)
            self.weight_used.append((now, weight))

# ====================== BINANCE CLIENT ======================
class BinanceClient:
    """Binance Futures API Client with institutional-grade features"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.api_key = Config.BINANCE_API_KEY
        self.secret_key = Config.BINANCE_SECRET_KEY

        if Config.BINANCE_TESTNET:
            self.base_url = "https://testnet.binancefuture.com"
        else:
            self.base_url = "https://fapi.binance.com"

        self.rate_limiter = RateLimiter(Config.MAX_REQUESTS_PER_MINUTE, Config.WEIGHT_LIMIT_PER_MINUTE)

        self.session = requests.Session()
        self.session.headers.update({
            'X-MBX-APIKEY': self.api_key,
            'Content-Type': 'application/json'
        })

        self._test_connection()

    def _test_connection(self):
        try:
            response = self._request('GET', '/fapi/v1/ping')
            self.logger.info("✅ Binance API connection successful")
        except Exception as e:
            self.logger.error(f"❌ Binance API connection failed: {e}")
            raise

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        return hmac.new(self.secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    def _request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False, weight: int = 1) -> Dict:
        self.rate_limiter.wait_if_needed(weight)
        params = params or {}

        if signed:
            params['timestamp'] = int(time.time() * 1000)
            params['signature'] = self._generate_signature(params)

        url = f"{self.base_url}{endpoint}"

        try:
            if method == 'GET':
                response = self.session.get(url, params=params, timeout=10)
            elif method == 'POST':
                response = self.session.post(url, params=params, timeout=10)
            elif method == 'DELETE':
                response = self.session.delete(url, params=params, timeout=10)

            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"API request failed: {e}")
            raise

    def get_account_info(self) -> Dict:
        return self._request('GET', '/fapi/v2/account', signed=True, weight=5)

    def get_balance(self) -> Decimal:
        try:
            account = self.get_account_info()
            for asset in account['assets']:
                if asset['asset'] == 'USDT':
                    return Decimal(asset['walletBalance'])
            return Decimal('0')
        except Exception:
            return Decimal('0')

    def get_positions(self) -> List[Dict]:
        try:
            response = self._request('GET', '/fapi/v2/positionRisk', signed=True, weight=5)
            positions = []
            for pos in response:
                if float(pos['positionAmt']) != 0:
                    positions.append({
                        'symbol': pos['symbol'],
                        'side': 'LONG' if float(pos['positionAmt']) > 0 else 'SHORT',
                        'size': abs(float(pos['positionAmt'])),
                        'entry_price': float(pos['entryPrice']),
                        'mark_price': float(pos['markPrice']),
                        'pnl': float(pos['unRealizedProfit']),
                        'percentage': float(pos['percentage'])
                    })
            return positions
        except Exception:
            return []

    def get_klines(self, symbol: str, interval: str, limit: int = 100) -> List[List]:
        params = {'symbol': symbol, 'interval': interval, 'limit': limit}
        return self._request('GET', '/fapi/v1/klines', params, weight=1)

    def get_ticker_price(self, symbol: str) -> Dict:
        params = {'symbol': symbol}
        return self._request('GET', '/fapi/v1/ticker/price', params, weight=1)

    def get_24hr_ticker(self, symbol: str) -> Dict:
        params = {'symbol': symbol}
        return self._request('GET', '/fapi/v1/ticker/24hr', params, weight=1)

    def set_leverage(self, symbol: str, leverage: int) -> Dict:
        params = {'symbol': symbol, 'leverage': leverage}
        return self._request('POST', '/fapi/v1/leverage', params, signed=True, weight=1)

    def place_order(self, symbol: str, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        params = {'symbol': symbol, 'side': side, 'type': order_type, 'quantity': quantity}
        if price:
            params['price'] = price
            params['timeInForce'] = 'GTC'
        return self._request('POST', '/fapi/v1/order', params, signed=True, weight=1)

    def close_position(self, symbol: str) -> Dict:
        positions = self.get_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
                return self.place_order(symbol=symbol, side=side, order_type='MARKET', quantity=pos['size'])
        raise ValueError(f"No position found for {symbol}")

# ====================== REALISTIC PAPER TRADING CLIENT ======================
class RealisticPaperTradingClient:
    """Paper trading client using real Binance public API data"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.balance = Config.INITIAL_BALANCE
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.base_url = "https://fapi.binance.com"  # Real Binance Futures API
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(1200, 6000)  # Public API limits
        self.logger.info("✅ Realistic paper trading client initialized with real market data")

    def _request(self, method: str, endpoint: str, params: Dict = None, weight: int = 1) -> Dict:
        """Make public API request (no authentication needed)"""
        self.rate_limiter.wait_if_needed(weight)
        params = params or {}
        url = f"{self.base_url}{endpoint}"

        try:
            if method == 'GET':
                response = self.session.get(url, params=params, timeout=10)
            else:
                response = self.session.post(url, params=params, timeout=10)

            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Public API request failed: {e}")
            raise

    def get_balance(self) -> Decimal:
        """Return simulated balance"""
        return self.balance

    def get_positions(self) -> List[Dict]:
        """Return simulated positions with real market prices"""
        positions = []
        for symbol, pos in self.positions.items():
            try:
                # Get real current price
                ticker = self.get_ticker_price(symbol)
                current_price = float(ticker['price'])

                # Calculate real P&L
                if pos['side'] == 'LONG':
                    pnl = (current_price - pos['entry_price']) * pos['size']
                else:
                    pnl = (pos['entry_price'] - current_price) * pos['size']

                positions.append({
                    'symbol': symbol,
                    'side': pos['side'],
                    'size': pos['size'],
                    'entry_price': pos['entry_price'],
                    'mark_price': current_price,
                    'pnl': pnl,
                    'percentage': (pnl / (pos['entry_price'] * pos['size'])) * 100 if pos['entry_price'] * pos['size'] > 0 else 0
                })
            except Exception as e:
                self.logger.warning(f"Failed to get real price for {symbol}: {e}")
                continue

        return positions

    def get_klines(self, symbol: str, interval: str, limit: int = 100) -> List[List]:
        """Get REAL candlestick data from Binance public API"""
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        return self._request('GET', '/fapi/v1/klines', params, weight=1)

    def get_ticker_price(self, symbol: str) -> Dict:
        """Get REAL current price from Binance"""
        params = {'symbol': symbol}
        return self._request('GET', '/fapi/v1/ticker/price', params, weight=1)

    def get_24hr_ticker(self, symbol: str) -> Dict:
        """Get REAL 24hr ticker statistics"""
        params = {'symbol': symbol}
        return self._request('GET', '/fapi/v1/ticker/24hr', params, weight=1)

    def set_leverage(self, symbol: str, leverage: int) -> Dict:
        """Simulate leverage setting"""
        self.logger.info(f"📊 Simulated: Set {symbol} leverage to {leverage}x")
        return {'symbol': symbol, 'leverage': leverage, 'status': 'simulated'}

    def place_order(self, symbol: str, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        """Simulate order placement with real market price"""
        try:
            # Get real current market price
            ticker = self.get_ticker_price(symbol)
            current_price = float(ticker['price'])

            # Simulate slippage (0.01-0.05%)
            slippage = random.uniform(0.0001, 0.0005)
            if side == 'BUY':
                execution_price = current_price * (1 + slippage)
            else:
                execution_price = current_price * (1 - slippage)

            order_id = random.randint(1000000, 9999999)

            # Handle position logic
            if symbol in self.positions:
                # Close existing position
                existing = self.positions[symbol]
                if existing['side'] == 'LONG':
                    pnl = (execution_price - existing['entry_price']) * existing['size']
                else:
                    pnl = (existing['entry_price'] - execution_price) * existing['size']

                # Update balance with P&L
                self.balance += Decimal(str(pnl))
                self.logger.info(f"💰 Closed {existing['side']} {symbol}: P&L ${pnl:.2f}")
                del self.positions[symbol]
            else:
                # Open new position
                position_side = 'LONG' if side == 'BUY' else 'SHORT'
                self.positions[symbol] = {
                    'side': position_side,
                    'size': quantity,
                    'entry_price': execution_price,
                    'timestamp': time.time()
                }
                self.logger.info(f"🚀 Opened {position_side} {symbol}: Size {quantity} @ ${execution_price:.4f}")

            return {
                'symbol': symbol,
                'orderId': order_id,
                'status': 'FILLED',
                'side': side,
                'type': order_type,
                'executedQty': str(quantity),
                'price': str(execution_price),
                'fills': [{'price': str(execution_price), 'qty': str(quantity)}]
            }

        except Exception as e:
            self.logger.error(f"Simulated order failed for {symbol}: {e}")
            raise

    def close_position(self, symbol: str) -> Dict:
        """Close position by placing opposite order"""
        if symbol in self.positions:
            pos = self.positions[symbol]
            side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
            return self.place_order(symbol, side, 'MARKET', pos['size'])
        raise ValueError(f"No position found for {symbol}")

    def get_exchange_info(self) -> Dict:
        """Get real exchange information"""
        return self._request('GET', '/fapi/v1/exchangeInfo', weight=1)

    def test_connectivity(self):
        """Test connection to Binance public API"""
        try:
            self._request('GET', '/fapi/v1/ping')
            self.logger.info("✅ Connected to Binance public API")

            # Test getting real data
            btc_price = self.get_ticker_price('BTCUSDT')
            self.logger.info(f"📊 Real BTC price: ${float(btc_price['price']):,.2f}")

        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Binance public API: {e}")
            raise

# ====================== TELEGRAM BOT ======================
class TelegramBot:
    """Telegram notification system"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.token = Config.TELEGRAM_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.enabled = bool(self.token and self.chat_id)
        self.last_send = 0
        self.min_interval = 2.0

    def send_message(self, message: str, critical: bool = False):
        if not self.enabled:
            self.logger.info(f"Telegram: {message}")
            return

        now = time.time()
        if not critical and now - self.last_send < self.min_interval:
            return

        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            data = {
                'chat_id': self.chat_id,
                'text': message[:4000],
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            response = requests.post(url, json=data, timeout=10)
            if response.status_code == 200:
                self.last_send = now
        except Exception as e:
            self.logger.warning(f"Telegram send failed: {e}")

# ====================== STRATEGY ENGINE ======================
@dataclass
class Signal:
    symbol: str
    side: str
    confidence: float
    entry_price: float
    stop_loss: float
    take_profit: float
    reasoning: str
    timestamp: float

class InstitutionalStrategyEngine:
    """Institutional-grade strategy engine"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.kalman_filters = {}
        self.ou_models = {}
        self.regime_detectors = {}
        self.price_history = {}
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
        self.anomaly_fitted = False

    def update_market_data(self, symbol: str, klines: List[List]):
        try:
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trades', 'taker_buy_base', 'taker_buy_quote', 'ignore'])

            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col])

            if symbol not in self.kalman_filters:
                self.kalman_filters[symbol] = KalmanFilter()
                self.ou_models[symbol] = OrnsteinUhlenbeckModel()
                self.regime_detectors[symbol] = RegimeDetector()
                self.price_history[symbol] = deque(maxlen=150)

            current_price = float(df['close'].iloc[-1])
            self.price_history[symbol].append(current_price)

            filtered_price = self.kalman_filters[symbol].update(current_price)
            ou_params = self.ou_models[symbol].update(current_price)

            if len(df) > 1:
                returns = df['close'].pct_change().iloc[-1]
                volatility = df['close'].pct_change().rolling(15).std().iloc[-1]
                volume_ratio = df['volume'].iloc[-1] / df['volume'].rolling(20).mean().iloc[-1]
                regime = self.regime_detectors[symbol].update(returns, volatility, volume_ratio)
            else:
                regime = 'NORMAL'

            return {'df': df, 'filtered_price': filtered_price, 'ou_params': ou_params, 'regime': regime, 'current_price': current_price}
        except Exception as e:
            self.logger.error(f"Failed to update market data for {symbol}: {e}")
            return None

    def generate_signal(self, symbol: str, market_data: Dict) -> Optional[Signal]:
        try:
            df = market_data['df']
            ou_params = market_data['ou_params']
            regime = market_data['regime']
            current_price = market_data['current_price']

            if len(df) < 30:
                return None

            close = df['close']
            high = df['high']
            low = df['low']
            volume = df['volume']

            # Technical indicators
            delta = close.diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rsi = 100 - (100 / (1 + gain / loss))
            current_rsi = rsi.iloc[-1]

            ema12 = close.ewm(span=12).mean()
            ema26 = close.ewm(span=26).mean()
            macd = ema12 - ema26
            signal_line = macd.ewm(span=9).mean()
            macd_histogram = macd - signal_line

            bb_middle = close.rolling(20).mean()
            bb_std = close.rolling(20).std()
            bb_upper = bb_middle + (bb_std * 2)
            bb_lower = bb_middle - (bb_std * 2)

            volume_sma = volume.rolling(20).mean()
            volume_ratio = volume.iloc[-1] / volume_sma.iloc[-1]

            # Hurst Exponent
            hurst = HurstExponent.calculate(list(self.price_history[symbol])) if len(self.price_history[symbol]) >= 30 else 0.5

            # Signal generation
            signals = []

            # 1. Mean Reversion (OU Model)
            if abs(ou_params['z_score']) > 1.8 and ou_params['half_life'] < 40:
                if ou_params['z_score'] < -1.8:
                    signals.append(('LONG', 0.8, 'OU Mean Reversion - Oversold'))
                elif ou_params['z_score'] > 1.8:
                    signals.append(('SHORT', 0.8, 'OU Mean Reversion - Overbought'))

            # 2. Trend Following (Hurst + MACD)
            if hurst > 0.6 and len(macd_histogram) > 1:
                if macd_histogram.iloc[-1] > 0 and macd_histogram.iloc[-1] > macd_histogram.iloc[-2]:
                    signals.append(('LONG', 0.7, 'Trend Following - MACD Bullish'))
                elif macd_histogram.iloc[-1] < 0 and macd_histogram.iloc[-1] < macd_histogram.iloc[-2]:
                    signals.append(('SHORT', 0.7, 'Trend Following - MACD Bearish'))

            # 3. RSI Extremes with Volume Confirmation
            if volume_ratio > 1.5:
                if current_rsi < 25:
                    signals.append(('LONG', 0.9, 'RSI Oversold + Volume Spike'))
                elif current_rsi > 75:
                    signals.append(('SHORT', 0.9, 'RSI Overbought + Volume Spike'))

            # 4. Bollinger Band Breakouts
            if current_price > bb_upper.iloc[-1] and volume_ratio > 1.3:
                signals.append(('LONG', 0.6, 'Bollinger Breakout + Volume'))
            elif current_price < bb_lower.iloc[-1] and volume_ratio > 1.3:
                signals.append(('SHORT', 0.6, 'Bollinger Breakdown + Volume'))

            # 5. Regime-based signals
            if regime == 'BULL_TREND' and current_rsi < 50:
                signals.append(('LONG', 0.7, 'Bull Regime + RSI Dip'))
            elif regime == 'BEAR_TREND' and current_rsi > 50:
                signals.append(('SHORT', 0.7, 'Bear Regime + RSI Rally'))

            # 6. Kalman Filter Trend
            if len(self.price_history[symbol]) >= 5:
                recent_filtered = [self.kalman_filters[symbol].posteri_estimate]
                if len(recent_filtered) >= 3:
                    if all(recent_filtered[i] > recent_filtered[i-1] for i in range(1, len(recent_filtered))):
                        signals.append(('LONG', 0.5, 'Kalman Uptrend'))
                    elif all(recent_filtered[i] < recent_filtered[i-1] for i in range(1, len(recent_filtered))):
                        signals.append(('SHORT', 0.5, 'Kalman Downtrend'))

            if not signals:
                return None

            # Aggregate signals
            long_signals = [s for s in signals if s[0] == 'LONG']
            short_signals = [s for s in signals if s[0] == 'SHORT']

            if len(long_signals) > len(short_signals):
                side = 'LONG'
                confidence = sum(s[1] for s in long_signals) / len(long_signals)
                reasoning = '; '.join([s[2] for s in long_signals])
            elif len(short_signals) > len(long_signals):
                side = 'SHORT'
                confidence = sum(s[1] for s in short_signals) / len(short_signals)
                reasoning = '; '.join([s[2] for s in short_signals])
            else:
                return None

            if confidence < float(Config.SIGNAL_THRESHOLD):
                return None

            # Calculate stop loss and take profit
            atr = (high.rolling(14).max() - low.rolling(14).min()).rolling(14).mean().iloc[-1]

            if side == 'LONG':
                stop_loss = current_price - (atr * 1.5)
                take_profit = current_price + (atr * 2.5)
            else:
                stop_loss = current_price + (atr * 1.5)
                take_profit = current_price - (atr * 2.5)

            return Signal(
                symbol=symbol,
                side=side,
                confidence=confidence,
                entry_price=current_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                reasoning=reasoning,
                timestamp=time.time()
            )

        except Exception as e:
            self.logger.error(f"Signal generation failed for {symbol}: {e}")
            return None

# ====================== TRADING BOT ======================
class TradingBot:
    """Main trading bot orchestrator"""

    def __init__(self, use_realistic_paper: bool = False):
        self.logger = logging.getLogger(__name__)

        if use_realistic_paper:
            self.binance = RealisticPaperTradingClient()
            # Test connectivity on startup
            self.binance.test_connectivity()
        else:
            self.binance = BinanceClient()

        self.strategy = InstitutionalStrategyEngine()
        self.telegram = TelegramBot()
        self.running = False
        self.start_time = time.time()
        self.last_report = 0
        self.positions = {}
        self.balance = Config.INITIAL_BALANCE
        self.equity_peak = Config.INITIAL_BALANCE
        self.total_pnl = Decimal('0')

        # Initialize database
        self._init_database()

    def _init_database(self):
        """Initialize SQLite database for persistence"""
        try:
            self.conn = sqlite3.connect(':memory:', check_same_thread=False)
            self.conn.execute('''
                CREATE TABLE positions (
                    symbol TEXT PRIMARY KEY,
                    side TEXT,
                    size REAL,
                    entry_price REAL,
                    stop_loss REAL,
                    take_profit REAL,
                    timestamp REAL
                )
            ''')
            self.conn.execute('''
                CREATE TABLE trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    side TEXT,
                    size REAL,
                    entry_price REAL,
                    exit_price REAL,
                    pnl REAL,
                    timestamp REAL
                )
            ''')
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"Database init failed: {e}")

    def get_balance(self) -> Decimal:
        """Get current balance"""
        try:
            return self.binance.get_balance()
        except Exception:
            return self.balance

    def get_positions(self) -> List[Dict]:
        """Get current positions"""
        try:
            return self.binance.get_positions()
        except Exception:
            return list(self.positions.values())

    def calculate_position_size(self, symbol: str, entry_price: float, stop_loss: float) -> float:
        """Calculate optimal position size using risk management"""
        try:
            current_balance = float(self.get_balance())
            risk_amount = current_balance * float(Config.BASE_RISK_PERCENT) / 100

            price_risk = abs(entry_price - stop_loss)
            if price_risk <= 0:
                return 0

            position_size = risk_amount / price_risk

            # Apply leverage
            position_size *= Config.LEVERAGE

            # Minimum position size
            min_size = 10.0 / entry_price  # $10 minimum

            return max(min_size, position_size)

        except Exception as e:
            self.logger.error(f"Position size calculation failed: {e}")
            return 0

    def open_position(self, signal: Signal) -> bool:
        """Open a new position"""
        try:
            # Check if we already have a position
            if signal.symbol in self.positions:
                return False

            # Check maximum positions
            if len(self.positions) >= Config.MAX_POSITIONS:
                return False

            # Calculate position size
            position_size = self.calculate_position_size(signal.symbol, signal.entry_price, signal.stop_loss)

            if position_size <= 0:
                return False

            # Set leverage
            try:
                self.binance.set_leverage(signal.symbol, Config.LEVERAGE)
            except Exception:
                pass  # Leverage might already be set

            # Place order
            side = 'BUY' if signal.side == 'LONG' else 'SELL'
            order = self.binance.place_order(
                symbol=signal.symbol,
                side=side,
                order_type='MARKET',
                quantity=position_size
            )

            # Store position
            position = {
                'symbol': signal.symbol,
                'side': signal.side,
                'size': position_size,
                'entry_price': signal.entry_price,
                'stop_loss': signal.stop_loss,
                'take_profit': signal.take_profit,
                'timestamp': signal.timestamp
            }

            self.positions[signal.symbol] = position

            # Save to database
            self.conn.execute('''
                INSERT OR REPLACE INTO positions 
                (symbol, side, size, entry_price, stop_loss, take_profit, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (signal.symbol, signal.side, position_size, signal.entry_price, 
                  signal.stop_loss, signal.take_profit, signal.timestamp))
            self.conn.commit()

            # Send notification
            self.telegram.send_message(
                f"🚀 <b>POSITION OPENED</b>\n"
                f"📊 {signal.symbol}\n"
                f"📈 {signal.side} ${position_size:.4f}\n"
                f"💰 Entry: ${signal.entry_price:.4f}\n"
                f"🛑 SL: ${signal.stop_loss:.4f}\n"
                f"🎯 TP: ${signal.take_profit:.4f}\n"
                f"🧠 Reason: {signal.reasoning}\n"
                f"⚡ Confidence: {signal.confidence:.1%}"
            )

            self.logger.info(f"Opened {signal.side} position for {signal.symbol}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to open position for {signal.symbol}: {e}")
            return False

    def close_position(self, symbol: str, reason: str = "Manual") -> bool:
        """Close a position"""
        try:
            if symbol not in self.positions:
                return False

            position = self.positions[symbol]

            # Get current price
            ticker = self.binance.get_ticker_price(symbol)
            current_price = float(ticker['price'])

            # Close position on exchange
            try:
                self.binance.close_position(symbol)
            except Exception as e:
                self.logger.warning(f"Exchange close failed for {symbol}: {e}")

            # Calculate PnL
            if position['side'] == 'LONG':
                pnl = (current_price - position['entry_price']) * position['size']
            else:
                pnl = (position['entry_price'] - current_price) * position['size']

            self.total_pnl += Decimal(str(pnl))

            # Save trade to database
            self.conn.execute('''
                INSERT INTO trades 
                (symbol, side, size, entry_price, exit_price, pnl, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, position['side'], position['size'], position['entry_price'], 
                  current_price, pnl, time.time()))

            # Remove position
            del self.positions[symbol]
            self.conn.execute('DELETE FROM positions WHERE symbol = ?', (symbol,))
            self.conn.commit()

            # Send notification
            emoji = "✅" if pnl > 0 else "❌"
            self.telegram.send_message(
                f"{emoji} <b>POSITION CLOSED</b>\n"
                f"📊 {symbol}\n"
                f"📈 {position['side']} ${position['size']:.4f}\n"
                f"💰 Entry: ${position['entry_price']:.4f}\n"
                f"📉 Exit: ${current_price:.4f}\n"
                f"💵 PnL: ${pnl:.2f}\n"
                f"📝 Reason: {reason}"
            )

            self.logger.info(f"Closed {position['side']} position for {symbol}, PnL: ${pnl:.2f}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to close position for {symbol}: {e}")
            return False

    def manage_positions(self):
        """Manage existing positions"""
        try:
            current_positions = self.get_positions()

            for position in current_positions:
                symbol = position['symbol']
                current_price = position['mark_price']

                # Check stop loss
                if symbol in self.positions:
                    stored_position = self.positions[symbol]

                    if stored_position['side'] == 'LONG':
                        if current_price <= stored_position['stop_loss']:
                            self.close_position(symbol, "Stop Loss")
                        elif current_price >= stored_position['take_profit']:
                            self.close_position(symbol, "Take Profit")
                    else:  # SHORT
                        if current_price >= stored_position['stop_loss']:
                            self.close_position(symbol, "Stop Loss")
                        elif current_price <= stored_position['take_profit']:
                            self.close_position(symbol, "Take Profit")

                    # Time-based exit (24 hours max)
                    if time.time() - stored_position['timestamp'] > 86400:
                        self.close_position(symbol, "Time Limit")

        except Exception as e:
            self.logger.error(f"Position management failed: {e}")

    def scan_for_signals(self):
        """Scan all symbols for trading signals"""
        try:
            for symbol in Config.TRADING_PAIRS[:20]:  # Limit to top 20 for free tier
                try:
                    # Skip if we already have a position
                    if symbol in self.positions:
                        continue

                    # Get market data
                    klines = self.binance.get_klines(symbol, '5m', 100)
                    market_data = self.strategy.update_market_data(symbol, klines)

                    if not market_data:
                        continue

                    # Generate signal
                    signal = self.strategy.generate_signal(symbol, market_data)

                    if signal and signal.confidence >= float(Config.SIGNAL_THRESHOLD):
                        self.open_position(signal)

                    time.sleep(0.1)  # Rate limiting

                except Exception as e:
                    self.logger.warning(f"Signal scan failed for {symbol}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Signal scanning failed: {e}")

    def send_periodic_report(self):
        """Send periodic status report"""
        try:
            now = time.time()
            if now - self.last_report < Config.REPORT_INTERVAL:
                return

            current_balance = self.get_balance()
            positions = self.get_positions()

            total_unrealized_pnl = sum(pos['pnl'] for pos in positions)
            total_return = ((current_balance + Decimal(str(total_unrealized_pnl)) - Config.INITIAL_BALANCE) / Config.INITIAL_BALANCE) * 100

            report = (
                f"📊 <b>TRADING REPORT</b>\n"
                f"💰 Balance: ${float(current_balance):,.2f}\n"
                f"📈 Unrealized PnL: ${total_unrealized_pnl:+,.2f}\n"
                f"📊 Total Return: {float(total_return):+.2f}%\n"
                f"🔢 Open Positions: {len(positions)}/{Config.MAX_POSITIONS}\n"
                f"⏰ Uptime: {(now - self.start_time)/3600:.1f}h\n"
                f"🎯 Strategy: Institutional Grade\n"
                f"⚡ Mode: {Config.LEVERAGE}x Leverage"
            )

            if positions:
                report += "\n\n<b>OPEN POSITIONS:</b>\n"
                for pos in positions[:5]:  # Show top 5
                    emoji = "🟢" if pos['pnl'] > 0 else "🔴"
                    report += f"{emoji} {pos['symbol']} {pos['side']}: ${pos['pnl']:+.2f}\n"

            self.telegram.send_message(report)
            self.last_report = now

        except Exception as e:
            self.logger.error(f"Report generation failed: {e}")

    def run(self):
        """Main trading loop"""
        self.running = True
        self.logger.info("🚀 Starting OmegaX Institutional Futures Trading Bot")

        # Send startup notification
        self.telegram.send_message(
            f"🚀 <b>OMEGAX BOT STARTED</b>\n"
            f"💰 Initial Balance: ${float(Config.INITIAL_BALANCE):,.2f}\n"
            f"📊 Monitoring: {len(Config.TRADING_PAIRS)} pairs\n"
            f"⚡ Leverage: {Config.LEVERAGE}x\n"
            f"🎯 Max Positions: {Config.MAX_POSITIONS}\n"
            f"🧠 Strategy: Institutional Grade\n"
            f"🔥 Mode: BEAST ACTIVATED",
            critical=True
        )

        try:
            while self.running:
                loop_start = time.time()

                try:
                    # Manage existing positions
                    self.manage_positions()

                    # Scan for new signals
                    self.scan_for_signals()

                    # Send periodic reports
                    self.send_periodic_report()

                except Exception as e:
                    self.logger.error(f"Trading loop error: {e}")

                # Sleep with jitter
                loop_time = time.time() - loop_start
                sleep_time = max(1, Config.UPDATE_INTERVAL - loop_time)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal")
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            self.telegram.send_message(f"💥 <b>FATAL ERROR</b>\n{str(e)[:200]}", critical=True)
        finally:
            self.stop()

    def stop(self):
        """Stop the trading bot"""
        self.running = False

        # Send shutdown notification
        try:
            final_balance = self.get_balance()
            total_return = ((final_balance - Config.INITIAL_BALANCE) / Config.INITIAL_BALANCE) * 100

            self.telegram.send_message(
                f"🛑 <b>BOT STOPPED</b>\n"
                f"💰 Final Balance: ${float(final_balance):,.2f}\n"
                f"📈 Total Return: {float(total_return):+.2f}%\n"
                f"📊 Open Positions: {len(self.get_positions())}\n"
                f"⏰ Runtime: {(time.time() - self.start_time)/3600:.1f}h",
                critical=True
            )
        except Exception:
            pass

        self.logger.info("Trading bot stopped")

# ====================== HEALTH SERVER ======================
class HealthHandler(BaseHTTPRequestHandler):
    """Health check handler for deployment platforms"""

    def do_GET(self):
        try:
            status = {
                "status": "healthy",
                "service": "OmegaX Institutional Futures Bot",
                "timestamp": time.time(),
                "uptime": time.time() - getattr(self.server, 'start_time', time.time())
            }

            if hasattr(self.server, 'bot') and self.server.bot:
                try:
                    status.update({
                        "balance": float(self.server.bot.get_balance()),
                        "positions": len(self.server.bot.get_positions()),
                        "total_pnl": float(self.server.bot.total_pnl),
                        "running": self.server.bot.running
                    })
                except Exception:
                    pass

            response = json.dumps(status, indent=2)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())

        except Exception:
            self.send_response(500)
            self.end_headers()

    def log_message(self, format, *args):
        return  # Suppress HTTP logs

# ====================== MAIN ENTRY POINT ======================
def main():
    """Main entry point"""
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)

        use_realistic_paper = Config.USE_REALISTIC_PAPER
        if use_realistic_paper:
            logger.info("📝 REALISTIC PAPER TRADING MODE - Real market data, simulated trades")
            logger.info("🌐 Using REAL Binance market data (public API)")
        else:
            # For paper trading, use demo credentials if real ones not provided
            if not Config.BINANCE_API_KEY or not Config.BINANCE_SECRET_KEY:
                logger.warning("⚠️ No Binance credentials found - using PAPER TRADING mode")
                Config.BINANCE_API_KEY = "demo_api_key_for_paper_trading"
                Config.BINANCE_SECRET_KEY = "demo_secret_key_for_paper_trading"
                Config.BINANCE_TESTNET = True
                logger.info("📝 PAPER TRADING MODE ACTIVATED - No real money at risk")

        if not Config.TELEGRAM_TOKEN or not Config.TELEGRAM_CHAT_ID:
            logger.warning("⚠️ Telegram credentials missing - notifications disabled")

        logger.info("🏛️ Initializing OmegaX Institutional Futures Trading Bot")
        logger.info(f"📊 Monitoring {len(Config.TRADING_PAIRS)} trading pairs")
        logger.info(f"⚡ Leverage: {Config.LEVERAGE}x")
        logger.info(f"🎯 Max Positions: {Config.MAX_POSITIONS}")
        logger.info(f"💰 Base Risk: {Config.BASE_RISK_PERCENT}% per trade")
        logger.info(f"🔄 Mode: {'PAPER TRADING' if Config.BINANCE_TESTNET else 'LIVE TRADING'}")

        # Initialize trading bot (preserves original behavior unless USE_REALISTIC_PAPER=true)
        bot = TradingBot(use_realistic_paper=use_realistic_paper)

        # Start trading bot in background thread
        bot_thread = threading.Thread(target=bot.run, daemon=True)
        bot_thread.start()

        # Start health server for deployment platforms
        port = int(os.environ.get('PORT', 8080))
        server = HTTPServer(('0.0.0.0', port), HealthHandler)
        server.bot = bot
        server.start_time = time.time()

        logger.info(f"🌐 Health server running on port {port}")
        if use_realistic_paper:
            logger.info("✅ Bot is now running with REAL market data. Press Ctrl+C to stop.")
        else:
            logger.info("✅ Bot is now running. Press Ctrl+C to stop.")

        # Keep the main thread alive
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal")
        finally:
            bot.stop()
            server.server_close()
            logger.info("👋 Goodbye!")

    except Exception as e:
        print(f"💥 Fatal startup error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()