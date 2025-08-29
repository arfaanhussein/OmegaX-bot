# main.py - RAILWAY CLOUD COMPATIBLE DEPLOYMENT
"""
OmegaX Institutional Futures Trading Bot - Railway Cloud Ready
"""

import os
import sys
import time
import json
import logging
import threading
import sqlite3
import hmac
import hashlib
import requests
import random
import signal
from datetime import datetime, timedelta
from decimal import Decimal, getcontext, ROUND_HALF_UP
from typing import Dict, List, Optional, Any
from collections import deque, defaultdict
from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler
import warnings
warnings.filterwarnings('ignore')

# Railway-compatible import handling
try:
    import numpy as np
    import pandas as pd
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.ensemble import IsolationForest
    warnings.filterwarnings('ignore', category=RuntimeWarning)
    warnings.filterwarnings('ignore', category=FutureWarning)
except ImportError as e:
    print(f"❌ CRITICAL ERROR: Required packages missing: {e}")
    print("🔧 Add to requirements.txt: numpy pandas scikit-learn requests")
    sys.exit(1)

# FIXED: Proper decimal precision
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP
D = Decimal

# ====================== RAILWAY CLOUD CONFIGURATION ======================
class Config:
    """Railway Cloud optimized configuration"""
    
    # Railway Environment Variables
    PORT = int(os.environ.get('PORT', 8080))
    RAILWAY_ENVIRONMENT = os.environ.get('RAILWAY_ENVIRONMENT', 'development')
    
    # API Configuration
    BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', '')
    BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '')
    BINANCE_TESTNET = os.environ.get('BINANCE_TESTNET', 'false').lower() == 'true'
    USE_REALISTIC_PAPER = os.environ.get('USE_REALISTIC_PAPER', 'true').lower() == 'true'

    # Telegram Configuration
    TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '')
    TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

    # Trading Configuration
    INITIAL_BALANCE = D(os.environ.get('INITIAL_BALANCE', '1000'))
    BASE_RISK_PERCENT = D(os.environ.get('BASE_RISK_PERCENT', '5.0'))
    MAX_POSITIONS = int(os.environ.get('MAX_POSITIONS', '10'))
    LEVERAGE = int(os.environ.get('LEVERAGE', '10'))

    # Risk Management
    SIGNAL_THRESHOLD = D(os.environ.get('SIGNAL_THRESHOLD', '0.70'))
    STOP_LOSS_PERCENT = D(os.environ.get('STOP_LOSS_PERCENT', '1.2'))
    TAKE_PROFIT_PERCENT = D(os.environ.get('TAKE_PROFIT_PERCENT', '2.4'))
    TRAILING_TP_PERCENT = D(os.environ.get('TRAILING_TP_PERCENT', '2.0'))
    MAX_DRAWDOWN = D(os.environ.get('MAX_DRAWDOWN', '0.08'))

    # System Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    UPDATE_INTERVAL = int(os.environ.get('UPDATE_INTERVAL', '30'))
    REPORT_INTERVAL = int(os.environ.get('REPORT_INTERVAL', '300'))

    # Rate Limiting
    MAX_REQUESTS_PER_MINUTE = int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '500'))
    WEIGHT_LIMIT_PER_MINUTE = int(os.environ.get('WEIGHT_LIMIT_PER_MINUTE', '4000'))

    # Trading Pairs
    TRADING_PAIRS = [
        'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT', 'DOGEUSDT', 'MATICUSDT',
        'DOTUSDT', 'LTCUSDT', 'AVAXUSDT', 'LINKUSDT', 'UNIUSDT', 'ATOMUSDT', 'XLMUSDT', 'VETUSDT',
        'FILUSDT', 'ICPUSDT', 'HBARUSDT', 'APTUSDT', 'NEARUSDT', 'GRTUSDT', 'SANDUSDT', 'MANAUSDT',
        'FLOWUSDT', 'EGLDUSDT', 'XTZUSDT', 'THETAUSDT', 'AXSUSDT', 'AAVEUSDT', 'EOSUSDT', 'KLAYUSDT',
        'ALGOUSDT', 'CHZUSDT', 'ENJUSDT', 'ZILUSDT', 'BATUSDT', 'ZECUSDT', 'DASHUSDT', 'COMPUSDT',
        'YFIUSDT', 'SUSHIUSDT', 'SNXUSDT', 'MKRUSDT', 'CRVUSDT', 'BALUSDT', 'RENUSDT', 'KNCUSDT'
    ]

# ====================== RAILWAY LOGGING SETUP ======================
def setup_logging():
    """Railway-optimized logging setup"""
    level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

# ====================== THREAD-SAFE DATABASE MANAGER ======================
class DatabaseManager:
    """FIXED: Railway-compatible thread-safe database manager"""
    
    def __init__(self, db_path: str = '/tmp/omegax_trading.db'):
        self.db_path = db_path
        self._connections = {}
        self._lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        self._init_database()

    def _get_connection(self):
        """FIXED: Get thread-specific database connection"""
        thread_id = threading.get_ident()
        
        if thread_id not in self._connections:
            try:
                conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('PRAGMA synchronous=NORMAL')
                conn.execute('PRAGMA temp_store=MEMORY')
                self._connections[thread_id] = conn
            except Exception as e:
                self.logger.error(f"Database connection failed: {e}")
                conn = sqlite3.connect(':memory:', timeout=30.0)
                self._connections[thread_id] = conn
        
        return self._connections[thread_id]

    def _init_database(self):
        """Initialize database schema"""
        try:
            with self._lock:
                conn = self._get_connection()
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS positions (
                        symbol TEXT PRIMARY KEY,
                        side TEXT,
                        size REAL,
                        entry_price REAL,
                        stop_loss REAL,
                        take_profit REAL,
                        trailing_tp REAL,
                        timestamp REAL,
                        strategy TEXT
                    )
                ''')
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        side TEXT,
                        size REAL,
                        entry_price REAL,
                        exit_price REAL,
                        pnl REAL,
                        pnl_percent REAL,
                        exit_reason TEXT,
                        strategy TEXT,
                        confidence REAL,
                        timestamp REAL,
                        duration_seconds REAL
                    )
                ''')
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS performance_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp REAL,
                        balance REAL,
                        total_pnl REAL,
                        open_positions INTEGER,
                        daily_trades INTEGER,
                        daily_pnl REAL,
                        win_rate REAL
                    )
                ''')
                conn.commit()
                self.logger.info("✅ Database initialized successfully")
        except Exception as e:
            self.logger.error(f"Database init failed: {e}")

    def execute(self, query: str, params: tuple = None):
        """FIXED: Thread-safe database execution"""
        try:
            with self._lock:
                conn = self._get_connection()
                if params:
                    return conn.execute(query, params)
                return conn.execute(query)
        except Exception as e:
            self.logger.error(f"Database execute failed: {e}")
            return None

    def commit(self):
        """FIXED: Thread-safe commit"""
        try:
            with self._lock:
                conn = self._get_connection()
                conn.commit()
        except Exception as e:
            self.logger.error(f"Database commit failed: {e}")

    def close_all(self):
        """Close all connections"""
        try:
            for conn in self._connections.values():
                conn.close()
            self._connections.clear()
        except Exception:
            pass

# ====================== TELEGRAM BOT ======================
class TelegramBot:
    """Railway-optimized Telegram notification system"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.token = Config.TELEGRAM_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.enabled = bool(self.token and self.chat_id)
        self.last_send = 0
        self.min_interval = 3.0
        self._lock = threading.Lock()

    def send_message(self, message: str, critical: bool = False):
        if not self.enabled:
            self.logger.info(f"Telegram: {message}")
            return

        with self._lock:
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
                response = requests.post(url, json=data, timeout=15)
                if response.status_code == 200:
                    self.last_send = now
            except Exception as e:
                self.logger.warning(f"Telegram failed: {e}")

# ====================== RATE LIMITER ======================
class RateLimiter:
    """Railway-optimized rate limiter"""
    
    def __init__(self, max_requests_per_minute: int, max_weight_per_minute: int):
        self.max_requests = max_requests_per_minute
        self.max_weight = max_weight_per_minute
        self.requests = deque(maxlen=1000)
        self.weight_used = deque(maxlen=1000)
        self.lock = threading.RLock()
        self.consecutive_failures = 0

    def wait_if_needed(self, weight: int = 1):
        with self.lock:
            now = time.time()
            
            cutoff = now - 60
            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()
            while self.weight_used and self.weight_used[0][0] < cutoff:
                self.weight_used.popleft()

            current_requests = len(self.requests)
            current_weight = sum(w[1] for w in self.weight_used)

            if self.consecutive_failures > 0:
                backoff_time = min(30, 2 ** self.consecutive_failures)
                time.sleep(backoff_time)

            if current_requests >= self.max_requests * 0.9:
                sleep_time = 61 - (now - self.requests[0])
                if sleep_time > 0:
                    time.sleep(min(sleep_time, 30))

            if current_weight + weight > self.max_weight * 0.9:
                sleep_time = 61 - (now - self.weight_used[0][0])
                if sleep_time > 0:
                    time.sleep(min(sleep_time, 30))

            self.requests.append(now)
            self.weight_used.append((now, weight))

    def record_success(self):
        self.consecutive_failures = 0

    def record_failure(self):
        self.consecutive_failures = min(3, self.consecutive_failures + 1)

# ====================== REALISTIC PAPER TRADING CLIENT ======================
class RealisticPaperTradingClient:
    """FIXED: Railway-optimized paper trading with real market data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.balance = Config.INITIAL_BALANCE
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.base_url = "https://fapi.binance.com"
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(1000, 5000)
        self._lock = threading.Lock()
        
        self.session.headers.update({
            'User-Agent': 'OmegaX-Railway/1.0',
            'Connection': 'keep-alive'
        })
        
        self.logger.info("✅ Realistic paper trading client initialized")

    def test_connectivity(self):
        """Test connection to Binance public API"""
        try:
            response = self.session.get(f"{self.base_url}/fapi/v1/ping", timeout=10)
            response.raise_for_status()
            self.logger.info("✅ Connected to Binance public API")
        except Exception as e:
            self.logger.error(f"❌ Failed to connect: {e}")
            raise

    def get_balance(self) -> Decimal:
        with self._lock:
            return self.balance

    def get_positions(self) -> List[Dict]:
        with self._lock:
            positions = []
            for symbol, pos in self.positions.items():
                current_price = self._get_current_price(symbol)
                pnl = self._calculate_pnl(symbol, pos, current_price)
                
                positions.append({
                    'symbol': symbol,
                    'side': pos['side'],
                    'size': pos['size'],
                    'entry_price': pos['entry_price'],
                    'mark_price': current_price,
                    'pnl': pnl,
                    'percentage': (pnl / (pos['entry_price'] * pos['size'])) * 100 if pos['size'] > 0 else 0
                })
            return positions

    def _get_current_price(self, symbol: str) -> float:
        try:
            ticker = self.get_ticker_price(symbol)
            return float(ticker['price'])
        except Exception:
            return self._get_fallback_price(symbol)

    def _get_fallback_price(self, symbol: str) -> float:
        """FIXED: Use realistic fallback prices instead of random"""
        price_map = {
            'BTCUSDT': 45000, 'ETHUSDT': 2800, 'BNBUSDT': 300,             'XRPUSDT': 0.6, 'ADAUSDT': 0.5, 'SOLUSDT': 100, 'DOGEUSDT': 0.08, 'MATICUSDT': 0.9,
            'DOTUSDT': 7, 'LTCUSDT': 70, 'AVAXUSDT': 35, 'LINKUSDT': 15, 'UNIUSDT': 6, 'ATOMUSDT': 12,
            'XLMUSDT': 0.12, 'VETUSDT': 0.03, 'FILUSDT': 5, 'ICPUSDT': 12, 'HBARUSDT': 0.06,
            'APTUSDT': 8, 'NEARUSDT': 3, 'GRTUSDT': 0.15, 'SANDUSDT': 0.4, 'MANAUSDT': 0.5,
            'FLOWUSDT': 1.2, 'EGLDUSDT': 45, 'XTZUSDT': 1.1, 'THETAUSDT': 1.5, 'AXSUSDT': 8,
            'AAVEUSDT': 85, 'EOSUSDT': 1.2, 'KLAYUSDT': 0.2, 'ALGOUSDT': 0.25, 'CHZUSDT': 0.08,
            'ENJUSDT': 0.4, 'ZILUSDT': 0.025, 'BATUSDT': 0.3, 'ZECUSDT': 35, 'DASHUSDT': 45,
            'COMPUSDT': 55, 'YFIUSDT': 8500, 'SUSHIUSDT': 1.2, 'SNXUSDT': 3, 'MKRUSDT': 1200,
            'CRVUSDT': 0.8, 'BALUSDT': 4, 'RENUSDT': 0.08, 'KNCUSDT': 0.7
        }
        return price_map.get(symbol, 10.0)

    def _calculate_pnl(self, symbol: str, position: Dict, current_price: float) -> float:
        """FIXED: Correct PnL calculation without double leverage"""
        if position['side'] == 'LONG':
            return (current_price - position['entry_price']) * position['size']
        else:
            return (position['entry_price'] - current_price) * position['size']

    def get_klines(self, symbol: str, interval: str, limit: int = 100) -> List[List]:
        try:
            self.rate_limiter.wait_if_needed(1)
            params = {'symbol': symbol, 'interval': interval, 'limit': limit}
            response = self.session.get(f"{self.base_url}/fapi/v1/klines", params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.warning(f"Klines API failed for {symbol}: {e}")
            return self._generate_realistic_klines(symbol, limit)

    def _generate_realistic_klines(self, symbol: str, limit: int) -> List[List]:
        """FIXED: Generate realistic market data instead of random"""
        base_price = self._get_fallback_price(symbol)
        klines = []
        
        # Use more realistic price movements based on historical patterns
        for i in range(limit):
            # Simulate realistic market movements
            trend_factor = 0.0001 * (i - limit/2)  # Slight trend
            volatility = 0.005 + (0.01 * abs(trend_factor))  # Dynamic volatility
            
            price_change = np.random.normal(0, volatility)  # Normal distribution
            current_price = base_price * (1 + price_change + trend_factor)
            
            # Realistic OHLC generation
            high = current_price * (1 + abs(np.random.normal(0, 0.003)))
            low = current_price * (1 - abs(np.random.normal(0, 0.003)))
            open_price = current_price * (1 + np.random.normal(0, 0.002))
            
            # Ensure OHLC logic
            high = max(high, current_price, open_price)
            low = min(low, current_price, open_price)
            
            volume = max(100, np.random.lognormal(5, 1))  # Log-normal volume distribution
            timestamp = int(time.time() * 1000) + i * 300000
            
            klines.append([
                timestamp,
                str(open_price),
                str(high),
                str(low),
                str(current_price),
                str(volume),
                timestamp + 300000,
                str(volume * current_price),
                random.randint(50, 300),
                str(volume * 0.6),
                str(volume * current_price * 0.6),
                "0"
            ])
        return klines

    def get_ticker_price(self, symbol: str) -> Dict:
        try:
            self.rate_limiter.wait_if_needed(1)
            params = {'symbol': symbol}
            response = self.session.get(f"{self.base_url}/fapi/v1/ticker/price", params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception:
            base_price = self._get_fallback_price(symbol)
            return {'price': str(base_price)}

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        return True

    def place_order(self, symbol: str, side: str, order_type: str, quantity: float, price: float = None) -> Optional[Dict]:
        """FIXED: Simulate order placement with proper balance management"""
        try:
            with self._lock:
                current_price = self._get_current_price(symbol)
                order_id = random.randint(1000000, 9999999)
                
                # FIXED: Calculate margin required correctly
                notional_value = current_price * quantity
                margin_required = D(str(notional_value)) / D(str(Config.LEVERAGE))
                
                if margin_required > self.balance:
                    self.logger.warning(f"Insufficient balance for {symbol}: need {margin_required}, have {self.balance}")
                    return None
                
                if symbol not in self.positions:
                    self.positions[symbol] = {
                        'side': 'LONG' if side == 'BUY' else 'SHORT',
                        'size': quantity,
                        'entry_price': current_price,
                        'timestamp': time.time()
                    }
                    
                    self.balance -= margin_required
                    self.logger.info(f"Paper order: {side} {quantity:.6f} {symbol} @ {current_price:.4f}")
                    
                return {'orderId': order_id, 'status': 'FILLED'}
                
        except Exception as e:
            self.logger.error(f"Paper order failed: {e}")
            return None

    def close_position(self, symbol: str) -> bool:
        """FIXED: Simulate position closure with correct PnL calculation"""
        try:
            with self._lock:
                if symbol not in self.positions:
                    return True
                    
                position = self.positions[symbol]
                current_price = self._get_current_price(symbol)
                
                # FIXED: Calculate PnL correctly without double leverage
                pnl = self._calculate_pnl(symbol, position, current_price)
                
                # Return margin + PnL to balance
                notional_value = position['entry_price'] * position['size']
                margin_used = D(str(notional_value)) / D(str(Config.LEVERAGE))
                
                # FIXED: PnL is already leveraged from position size, don't apply leverage again
                self.balance += margin_used + D(str(pnl))
                
                self.logger.info(f"Paper close: {symbol} PnL: {pnl:.2f}")
                del self.positions[symbol]
                return True
                
        except Exception as e:
            self.logger.error(f"Paper close failed: {e}")
            return False

# ====================== BINANCE CLIENT ======================
class BinanceClient:
    """Railway-optimized Binance Futures API Client"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.api_key = Config.BINANCE_API_KEY
        self.secret_key = Config.BINANCE_SECRET_KEY
        
        self.base_url = "https://testnet.binancefuture.com" if Config.BINANCE_TESTNET else "https://fapi.binance.com"
        self.rate_limiter = RateLimiter(Config.MAX_REQUESTS_PER_MINUTE, Config.WEIGHT_LIMIT_PER_MINUTE)
        
        self.session = requests.Session()
        self.session.headers.update({
            'X-MBX-APIKEY': self.api_key,
            'Content-Type': 'application/json',
            'User-Agent': 'OmegaX-Railway/1.0'
        })

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        return hmac.new(
            self.secret_key.encode('utf-8'), 
            query_string.encode('utf-8'), 
            hashlib.sha256
        ).hexdigest()

    def _request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False, weight: int = 1) -> Optional[Dict]:
        self.rate_limiter.wait_if_needed(weight)
        params = params or {}

        if signed:
            if not self.api_key or not self.secret_key:
                return None
            params['timestamp'] = int(time.time() * 1000)
            params['signature'] = self._generate_signature(params)

        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(3):
            try:
                response = self.session.request(method, url, params=params, timeout=15)
                
                if response.status_code == 200:
                    self.rate_limiter.record_success()
                    return response.json()
                elif response.status_code == 429:
                    self.rate_limiter.record_failure()
                    time.sleep(60)
                    continue
                else:
                    self.rate_limiter.record_failure()
                    return None
                    
            except Exception as e:
                self.logger.warning(f"Request failed attempt {attempt + 1}: {e}")
                time.sleep(2 ** attempt)
                
        return None

    def get_balance(self) -> Decimal:
        try:
            if not self.api_key:
                return Config.INITIAL_BALANCE
            result = self._request('GET', '/fapi/v2/balance', signed=True)
            if result:
                usdt_balance = next((item for item in result if item['asset'] == 'USDT'), None)
                if usdt_balance:
                    return D(str(usdt_balance['balance']))
            return Config.INITIAL_BALANCE
        except Exception:
            return Config.INITIAL_BALANCE

    def get_positions(self) -> List[Dict]:
        try:
            if not self.api_key:
                return []
            result = self._request('GET', '/fapi/v2/positionRisk', signed=True)
            return result if result else []
        except Exception:
            return []

    def get_klines(self, symbol: str, interval: str, limit: int = 100) -> List[List]:
        try:
            params = {'symbol': symbol, 'interval': interval, 'limit': limit}
            result = self._request('GET', '/fapi/v1/klines', params=params, weight=1)
            return result if result else []
        except Exception:
            return []

    def get_ticker_price(self, symbol: str) -> Dict:
        try:
            params = {'symbol': symbol}
            result = self._request('GET', '/fapi/v1/ticker/price', params=params, weight=1)
            return result if result else {'price': '50000'}
        except Exception:
            return {'price': '50000'}

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        try:
            if not self.api_key:
                return True
            params = {'symbol': symbol, 'leverage': leverage}
            result = self._request('POST', '/fapi/v1/leverage', params=params, signed=True, weight=1)
            return bool(result)
        except Exception:
            return False

    def place_order(self, symbol: str, side: str, order_type: str, quantity: float, price: float = None) -> Optional[Dict]:
        try:
            if not self.api_key:
                return {'orderId': random.randint(1000000, 9999999), 'status': 'FILLED'}
            
            params = {
                'symbol': symbol,
                'side': side,
                'type': order_type,
                'quantity': f"{quantity:.6f}"
            }
            if price:
                params['price'] = f"{price:.6f}"
                
            result = self._request('POST', '/fapi/v1/order', params=params, signed=True, weight=1)
            return result
        except Exception:
            return None

    def close_position(self, symbol: str) -> bool:
        try:
            if not self.api_key:
                return True
                
            positions = self.get_positions()
            position = next((p for p in positions if p['symbol'] == symbol and float(p['positionAmt']) != 0), None)
            if not position:
                return True
            
            side = 'SELL' if float(position['positionAmt']) > 0 else 'BUY'
            quantity = abs(float(position['positionAmt']))
            
            result = self.place_order(symbol, side, 'MARKET', quantity)
            return bool(result)
        except Exception:
            return False

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

class KalmanFilter:
    def __init__(self, process_variance: float = 1e-5, measurement_variance: float = 1e-2):
        self.process_variance = process_variance
        self.measurement_variance = measurement_variance
        self.posteri_estimate: Optional[float] = None
        self.posteri_error_estimate: float = 1.0

    def update(self, measurement: float) -> float:
        if self.posteri_estimate is None:
            self.posteri_estimate = float(measurement)
            return float(measurement)

        priori_estimate = self.posteri_estimate
        priori_error_estimate = self.posteri_error_estimate + self.process_variance

        kalman_gain = priori_error_estimate / (priori_error_estimate + self.measurement_variance)
        self.posteri_estimate = priori_estimate + kalman_gain * (measurement - priori_estimate)
        self.posteri_error_estimate = (1 - kalman_gain) * priori_error_estimate
        return float(self.posteri_estimate)

class OrnsteinUhlenbeckModel:
    def __init__(self, window: int = 50):
        self.window = window
        self.prices = deque(maxlen=window)

    def update(self, price: float) -> Dict[str, float]:
        self.prices.append(float(price))
        if len(self.prices) < 5:
            return {'z_score': 0.0, 'mean': float(price), 'std': 1e-8}
        
        try:
            arr = np.array(self.prices, dtype=float)
            mean = float(np.mean(arr))
            std = float(np.std(arr))
            if std <= 0:
                std = 1e-8
            z = float((price - mean) / std)
            return {'z_score': z, 'mean': mean, 'std': std}
        except Exception:
            return {'z_score': 0.0, 'mean': float(price), 'std': 1e-8}

class RegimeDetector:
    def __init__(self):
        self.state = 'NORMAL'

    def update(self, returns: float, volatility: float, volume_ratio: float) -> str:
        try:
            returns = 0.0 if pd.isna(returns) or not np.isfinite(returns) else float(returns)
            volatility = 0.02 if pd.isna(volatility) or volatility <= 0 or not np.isfinite(volatility) else float(volatility)
            volume_ratio = 1.0 if pd.isna(volume_ratio) or volume_ratio <= 0 or not np.isfinite(volume_ratio) else float(volume_ratio)
        except Exception:
            returns, volatility, volume_ratio = 0.0, 0.02, 1.0

        if returns > 0.002 and volatility < 0.025 and volume_ratio >= 1.2:
            self.state = 'BULL_TREND'
        elif returns < -0.002 and volatility < 0.025 and volume_ratio >= 1.2:
            self.state = 'BEAR_TREND'
        elif volatility > 0.06:
            self.state = 'HIGH_VOLATILITY'
        else:
            self.state = 'NORMAL'
        return self.state

class InstitutionalStrategyEngine:
    """FIXED: Railway-optimized strategy engine with memory management"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.kalman_filters = {}
        self.ou_models = {}
        self.regime_detectors = {}
        self.price_history = {}
        self._lock = threading.Lock()
        
        # FIXED: Memory management for Railway
        self.max_symbols = 30  # Reduced for Railway memory limits
        self.cleanup_interval = 1800  # 30 minutes
        self.last_cleanup = time.time()

    def _cleanup_old_data(self):
        """FIXED: Prevent memory leaks by cleaning old symbol data"""
        if time.time() - self.last_cleanup < self.cleanup_interval:
            return
            
        with self._lock:
            if len(self.kalman_filters) > self.max_symbols:
                active_symbols = set(Config.TRADING_PAIRS[:self.max_symbols])
                
                for symbol in list(self.kalman_filters.keys()):
                    if symbol not in active_symbols:
                        self.kalman_filters.pop(symbol, None)
                        self.ou_models.pop(symbol, None)
                        self.regime_detectors.pop(symbol, None)
                        self.price_history.pop(symbol, None)
                        
            self.last_cleanup = time.time()

    def update_market_data(self, symbol: str, klines: List[List]):
        """FIXED: Memory-efficient market data processing"""
        try:
            self._cleanup_old_data()
            
            if not klines or len(klines) < 20:
                return None

            # FIXED: Efficient DataFrame creation with proper cleanup
            try:
                df = pd.DataFrame(klines[-50:], columns=[  # Limit to last 50 candles
                    'timestamp', 'open', 'high', 'low', 'close', 'volume', 
                    'close_time', 'quote_volume', 'trades', 'taker_buy_base', 
                    'taker_buy_quote', 'ignore'
                ])

                numeric_cols = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df = df.dropna(subset=numeric_cols)
                
                if len(df) < 20:
                    return None
                    
            except Exception as e:
                self.logger.error(f"DataFrame creation failed for {symbol}: {e}")
                return None

            with self._lock:
                if symbol not in self.kalman_filters:
                    self.kalman_filters[symbol] = KalmanFilter()
                    self.ou_models[symbol] = OrnsteinUhlenbeckModel()
                    self.regime_detectors[symbol] = RegimeDetector()
                    self.price_history[symbol] = deque(maxlen=50)  # Reduced size

                current_price = float(df['close'].iloc[-1])
                self.price_history[symbol].append(current_price)

                filtered_price = self.kalman_filters[symbol].update(current_price)
                ou_params = self.ou_models[symbol].update(current_price)

                returns = 0.0
                volatility = 0.02
                volume_ratio = 1.0
                
                try:
                    if len(df) > 1:
                        returns_series = df['close'].pct_change()
                        returns = returns_series.iloc[-1]
                        returns = 0.0 if pd.isna(returns) or not np.isfinite(returns) else float(returns)
                        
                        vol_window = min(20, len(df))
                        volatility_series = returns_series.rolling(vol_window).std()
                        volatility = volatility_series.iloc[-1]
                        volatility = 0.02 if pd.isna(volatility) or not np.isfinite(volatility) else float(volatility)
                        
                        vol_mean = df['volume'].rolling(vol_window).mean().iloc[-1]
                        if vol_mean > 0 and not pd.isna(vol_mean):
                            volume_ratio = df['volume'].iloc[-1] / vol_mean
                            volume_ratio = 1.0 if pd.isna(volume_ratio) or not np.isfinite(volume_ratio) else float(volume_ratio)
                except Exception as e:
                    self.logger.warning(f"Market metrics calculation failed for {symbol}: {e}")

                regime = self.regime_detectors[symbol].update(returns, volatility, volume_ratio)

            # FIXED: Explicit DataFrame cleanup for memory management
            del df
            
            return {
                'filtered_price': filtered_price, 
                'ou_params': ou_params, 
                'regime': regime, 
                'current_price': current_price,
                'returns': returns,
                'volatility': volatility,
                'volume_ratio': volume_ratio
            }
            
        except Exception as e:
            self.logger.error(f"Market data update failed for {symbol}: {e}")
            return None

    def generate_signal(self, symbol: str, market_data: Dict) -> Optional[Signal]:
        """Generate high-quality trading signals with conservative approach"""
        try:
            ou_params = market_data['ou_params']
            regime = market_data['regime']
            current_price = market_data['current_price']
            returns = market_data['returns']
            volatility = market_data['volatility']
            volume_ratio = market_data['volume_ratio']

            signals = []

            # STRATEGY 1: Conservative Mean Reversion
            if abs(ou_params['z_score']) > 2.0:
                if ou_params['z_score'] < -2.0 and volume_ratio > 1.3:
                    signals.append(('LONG', 0.85, 'Strong Mean Reversion Oversold'))
                elif ou_params['z_score'] > 2.0 and volume_ratio > 1.3:
                    signals.append(('SHORT', 0.85, 'Strong Mean Reversion Overbought'))

            # STRATEGY 2: Trend Following with Multiple Confirmations
            if regime in ['BULL_TREND', 'BEAR_TREND']:
                if regime == 'BULL_TREND' and returns > 0.01 and volume_ratio > 1.2:
                    signals.append(('LONG', 0.80, 'Multi-Confirmed Bull Trend'))
                elif regime == 'BEAR_TREND' and returns < -0.01 and volume_ratio > 1.2:
                    signals.append(('SHORT', 0.80, 'Multi-Confirmed Bear Trend'))

            # STRATEGY 3: Volume Breakout
            if volume_ratio > 2.0:
                if returns > 0.015:
                    signals.append(('LONG', 0.75, 'Volume Confirmed Breakout'))
                elif returns < -0.015:
                    signals.append(('SHORT', 0.75, 'Volume Confirmed Breakdown'))

            if not signals:
                return None

            # Conservative signal aggregation
            long_signals = [s for s in signals if s[0] == 'LONG']
            short_signals = [s for s in signals if s[0] == 'SHORT']

            if len(long_signals) >= 2 and len(long_signals) > len(short_signals):
                side = 'LONG'
                confidence = sum(s[1] for s in long_signals) / len(long_signals)
                reasoning = '; '.join([s[2] for s in long_signals])
            elif len(short_signals) >= 2 and len(short_signals) > len(long_signals):
                side = 'SHORT'
                confidence = sum(s[1] for s in short_signals) / len(short_signals)
                reasoning = '; '.join([s[2] for s in short_signals])
            else:
                return None

            if confidence < float(Config.SIGNAL_THRESHOLD):
                return None

            # Conservative SL/TP levels
            sl_percent = float(Config.STOP_LOSS_PERCENT) / 100
            tp_percent = float(Config.TAKE_PROFIT_PERCENT) / 100
            
            if side == 'LONG':
                stop_loss = current_price * (1 - sl_percent)
                take_profit = current_price * (1 + tp_percent)
            else:
                stop_loss = current_price * (1 + sl_percent)
                take_profit = current_price * (1 - tp_percent)

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
    """FIXED: Railway-optimized trading bot with proper risk management"""
    
    def __init__(self, use_realistic_paper: bool = False):
        self.logger = logging.getLogger(__name__)

        if use_realistic_paper:
            self.binance = RealisticPaperTradingClient()
            self.binance.test_connectivity()
        else:
            self.binance = BinanceClient()

        self.strategy = InstitutionalStrategyEngine()
        self.telegram = TelegramBot()
        self.db = DatabaseManager()
        
        self.running = False
        self.start_time = time.time()
        self.last_report = 0
        self.positions = {}
        self.balance = Config.INITIAL_BALANCE
        self.total_pnl = D('0')
        self.equity_peak = Config.INITIAL_BALANCE
        
        self.trailing_tp = {}
        self._lock = threading.Lock()

    def get_balance(self) -> Decimal:
        """Get current balance with thread safety"""
        try:
            return self.binance.get_balance()
        except Exception:
            return self.balance

    def get_positions(self) -> List[Dict]:
        """FIXED: Get current positions with thread safety"""
        try:
            positions = self.binance.get_positions()
            if positions:
                active_positions = []
                for p in positions:
                    try:
                        size = float(p.get('size', 0)) if 'size' in p else float(p.get('positionAmt', 0))
                        if abs(size) > 0.000001:
                            active_positions.append(p)
                    except (ValueError, TypeError):
                        continue
                return active_positions
        except Exception as e:
            self.logger.warning(f"Failed to get positions from exchange: {e}")

        # Fallback to local positions
        with self._lock:
            synthesized = []
            for symbol, p in self.positions.items():
                try:
                    ticker = self.binance.get_ticker_price(symbol)
                    current_price = float(ticker['price'])
                except Exception:
                    current_price = p['entry_price']

                pnl = self._calculate_position_pnl(p, current_price)
                
                synthesized.append({
                    'symbol': symbol,
                    'side': p['side'],
                    'size': p['size'],
                    'entry_price': p['entry_price'],
                    'mark_price': current_price,
                    'pnl': pnl,
                    'percentage': (pnl / (p['entry_price'] * p['size'])) * 100 if p['size'] > 0 else 0
                })

            return synthesized

    def _calculate_position_pnl(self, position: Dict, current_price: float) -> float:
        """Calculate position PnL with proper handling"""
        try:
            if position['side'] == 'LONG':
                return (current_price - position['entry_price']) * position['size']
            else:
                return (position['entry_price'] - current_price) * position['size']
        except Exception:
            return 0.0

    def calculate_position_size(self, symbol: str, entry_price: float, stop_loss: float) -> float:
        """FIXED: Calculate position size with correct risk management"""
        try:
            current_balance = float(self.get_balance())
            
            # Use 5% risk per trade as specified
            risk_amount = current_balance * float(Config.BASE_RISK_PERCENT) / 100
            
            # Calculate price risk
            price_risk = abs(entry_price - stop_loss)
            if price_risk <= 0:
                self.logger.warning(f"Invalid price risk for {symbol}: entry={entry_price}, sl={stop_loss}")
                return 0

            # FIXED: Calculate position size based on risk WITHOUT applying leverage
            # The leverage is handled by the exchange, not in position sizing
            position_size = risk_amount / price_risk
            
            # Ensure minimum viable position size
            min_notional = 10.0  # $10 minimum
            min_size = min_notional / entry_price
            
            # Maximum position size check
            max_notional = current_balance * 0.2  # Max 20% of balance per trade
            max_size = max_notional / entry_price
            
            # Final size calculation
            final_size = max(min_size, min(position_size, max_size))
            final_size = round(final_size, 6)
            
            self.logger.info(f"Position size for {symbol}: {final_size:.6f} (risk: ${risk_amount:.2f}, price_risk: {price_risk:.4f})")
            return final_size

        except Exception as e:
            self.logger.error(f"Position size calculation failed: {e}")
            return 0

    def open_position(self, signal: Signal) -> bool:
        """Open position with comprehensive validation and error handling"""
        try:
            with self._lock:
                if signal.symbol in self.positions:
                    self.logger.info(f"Position already exists for {signal.symbol}")
                    return False

                if len(self.positions) >= Config.MAX_POSITIONS:
                    self.logger.info(f"Max positions reached ({Config.MAX_POSITIONS})")
                    return False

                position_size = self.calculate_position_size(signal.symbol, signal.entry_price, signal.stop_loss)

                if position_size <= 0:
                    self.logger.warning(f"Invalid position size for {signal.symbol}: {position_size}")
                    return False

                if signal.entry_price <= 0 or signal.stop_loss <= 0 or signal.take_profit <= 0:
                    self.logger.warning(f"Invalid signal prices for {signal.symbol}")
                    return False

                # Set leverage first
                try:
                    leverage_success = self.binance.set_leverage(signal.symbol, Config.LEVERAGE)
                    if not leverage_success:
                        self.logger.warning(f"Failed to set leverage for {signal.symbol}")
                except Exception as e:
                    self.logger.warning(f"Leverage setting error for {signal.symbol}: {e}")

                # Place order
                side = 'BUY' if signal.side == 'LONG' else 'SELL'
                order_result = self.binance.place_order(
                    symbol=signal.symbol,
                    side=side,
                    order_type='MARKET',
                    quantity=position_size
                )

                if not order_result:
                    self.logger.error(f"Order placement failed for {signal.symbol}")
                    return False

                # Store position
                position = {
                    'symbol': signal.symbol,
                    'side': signal.side,
                    'size': position_size,
                    'entry_price': signal.entry_price,
                    'stop_loss': signal.stop_loss,
                    'take_profit': signal.take_profit,
                    'timestamp': signal.timestamp,
                    'strategy': signal.reasoning
                }

                self.positions[signal.symbol] = position
                self.trailing_tp[signal.symbol] = signal.take_profit

                # Save to database
                self.db.execute('''
                    INSERT OR REPLACE INTO positions 
                    (symbol, side, size, entry_price, stop_loss, take_profit, trailing_tp, timestamp, strategy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (signal.symbol, signal.side, position_size, signal.entry_price, 
                      signal.stop_loss, signal.take_profit, signal.take_profit, 
                      signal.timestamp, signal.reasoning))
                self.db.commit()

                # Calculate position value for notification
                position_value = signal.entry_price * position_size
                margin_used = position_value / Config.LEVERAGE

                # Send notification
                self.telegram.send_message(
                    f"🚀 <b>POSITION OPENED</b>\n"
                    f"📊 {signal.symbol}\n"
                    f"📈 {signal.side} {position_size:.6f}\n"
                    f"💰 Entry: ${signal.entry_price:.4f}\n"
                    f"🛑 SL: ${signal.stop_loss:.4f} (-{Config.STOP_LOSS_PERCENT}%)\n"
                    f"🎯 TP: ${signal.take_profit:.4f} (+{Config.TAKE_PROFIT_PERCENT}%)\n"
                    f"📊 Trailing: {Config.TRAILING_TP_PERCENT}%\n"
                    f"💼 Position Value: ${position_value:.2f}\n"
                    f"💳 Margin Used: ${margin_used:.2f}\n"
                    f"🧠 Strategy: {signal.reasoning}\n"
                    f"⚡ Confidence: {signal.confidence:.1%}\n"
                    f"🎯 Risk: {Config.BASE_RISK_PERCENT}% of balance"
                )

                self.logger.info(f"✅ Opened {signal.side} position for {signal.symbol} - Size: {position_size:.6f}")
                return True

        except Exception as e:
            self.logger.error(f"Failed to open position for {signal.symbol}: {e}")
            return False

    def close_position(self, symbol: str, reason: str = "Manual", exit_price: float = None) -> bool:
        """FIXED: Close position with correct PnL calculation"""
        try:
            with self._lock:
                if symbol not in self.positions:
                    self.logger.warning(f"No position found for {symbol}")
                    return False

                position = self.positions[symbol]

                if exit_price is None:
                    try:
                        ticker = self.binance.get_ticker_price(symbol)
                        exit_price = float(ticker['price'])
                    except Exception:
                        exit_price = position['entry_price']
                        self.logger.warning(f"Could not get current price for {symbol}, using entry price")

                # Close position on exchange
                try:
                    close_success = self.binance.close_position(symbol)
                    if not close_success:
                        self.logger.warning(f"Exchange close failed for {symbol}")
                except Exception as e:
                    self.logger.warning(f"Exchange close error for {symbol}: {e}")

                # FIXED: Calculate PnL correctly - leverage is already applied in position size
                raw_pnl = self._calculate_position_pnl(position, exit_price)
                
                # Calculate percentage return
                position_value = position['entry_price'] * position['size']
                pnl_percent = (raw_pnl / position_value) * 100 if position_value > 0 else 0
                
                # Calculate trade duration
                duration_seconds = time.time() - position['timestamp']
                
                self.total_pnl += D(str(raw_pnl))

                # Save trade to database
                self.db.execute('''
                    INSERT INTO trades 
                    (symbol, side, size, entry_price, exit_price, pnl, pnl_percent, 
                     exit_reason, strategy, confidence, timestamp, duration_seconds)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (symbol, position['side'], position['size'], position['entry_price'], 
                      exit_price, raw_pnl, pnl_percent, reason, position.get('strategy', ''),
                      0.0, time.time(), duration_seconds))

                # Remove position
                del self.positions[symbol]
                self.trailing_tp.pop(symbol, None)
                self.db.execute('DELETE FROM positions WHERE symbol = ?', (symbol,))
                self.db.commit()

                # Send notification
                emoji = "✅" if raw_pnl > 0 else "❌"
                duration_str = f"{duration_seconds/3600:.1f}h" if duration_seconds > 3600 else f"{duration_seconds/60:.0f}m"
                
                self.telegram.send_message(
                    f"{emoji} <b>POSITION CLOSED</b>\n"
                    f"📊 {symbol}\n"
                    f"📈 {position['side']} {position['size']:.6f}\n"
                    f"💰 Entry: ${position['entry_price']:.4f}\n"
                    f"📉 Exit: ${exit_price:.4f}\n"
                    f"💵 PnL: ${raw_pnl:.2f} ({pnl_percent:+.2f}%)\n"
                    f"📝 Reason: {reason}\n"
                    f"⏱️ Duration: {duration_str}\n"
                    f"⚡ Leverage: {Config.LEVERAGE}x"
                )

                self.logger.info(f"✅ Closed {position['side']} position for {symbol} - PnL: ${raw_pnl:.2f} ({pnl_percent:+.2f}%)")
                return True

        except Exception as e:
            self.logger.error(f"Failed to close position for {symbol}: {e}")
            return False

    def update_trailing_take_profit(self, symbol: str, current_price: float, position: Dict):
        """Implement 2% trailing take profit with proper logic"""
        try:
            if symbol not in self.trailing_tp:
                return

            current_tp = self.trailing_tp[symbol]
            trailing_percent = float(Config.TRAILING_TP_PERCENT) / 100

            if position['side'] == 'LONG':
                if current_price > current_tp:
                    potential_tp = current_price * (1 - trailing_percent)
                    if potential_tp > current_tp:
                        self.trailing_tp[symbol] = potential_tp
                        self.logger.info(f"Trailing TP updated for {symbol} LONG: ${potential_tp:.4f}")
            else:
                if current_price < current_tp:
                    potential_tp = current_price * (1 + trailing_percent)
                    if potential_tp < current_tp:
                        self.trailing_tp[symbol] = potential_tp
                        self.logger.info(f"Trailing TP updated for {symbol} SHORT: ${potential_tp:.4f}")

        except Exception as e:
            self.logger.error(f"Trailing TP update failed for {symbol}: {e}")

    def manage_positions(self):
        """FIXED: Enhanced position management with proper thread safety"""
        try:
            current_positions = self.get_positions()

            for position in current_positions:
                symbol = position['symbol']
                current_price = position['mark_price']

                if symbol in self.positions:
                    stored_position = self.positions[symbol]
                    
                    self.update_trailing_take_profit(symbol, current_price, stored_position)
                    
                    if stored_position['side'] == 'LONG':
                        if current_price <= stored_position['stop_loss']:
                            self.close_position(symbol, "Stop Loss", current_price)
                            continue
                        
                        if symbol in self.trailing_tp and current_price >= self.trailing_tp[symbol]:
                            self.close_position(symbol, "Trailing Take Profit", current_price)
                            continue
                            
                    else:  # SHORT position
                        if current_price >= stored_position['stop_loss']:
                            self.close_position(symbol, "Stop Loss", current_price)
                            continue
                        
                        if symbol in self.trailing_tp and current_price <= self.trailing_tp[symbol]:
                            self.close_position(symbol, "Trailing Take Profit", current_price)
                            continue

                    # Time-based exit (24 hours max)
                    if time.time() - stored_position['timestamp'] > 86400:
                        self.close_position(symbol, "Time Limit", current_price)
                        continue

                    # Emergency exit on extreme adverse movement
                    emergency_threshold = 0.05  # 5%
                    if stored_position['side'] == 'LONG':
                        emergency_price = stored_position['stop_loss'] * (1 - emergency_threshold)
                        if current_price <= emergency_price:
                            self.close_position(symbol, "Emergency Stop", current_price)
                            continue
                    else:
                        emergency_price = stored_position['stop_loss'] * (1 + emergency_threshold)
                        if current_price >= emergency_price:
                            self.close_position(symbol, "Emergency Stop", current_price)
                            continue

            self.check_drawdown_protection()

        except Exception as e:
            self.logger.error(f"Position management failed: {e}")

    def check_drawdown_protection(self):
        """FIXED: Emergency stop on excessive drawdown"""
        try:
            current_balance = self.get_balance()
            
            if current_balance > self.equity_peak:
                self.equity_peak = current_balance
            
            drawdown = (self.equity_peak - current_balance) / self.equity_peak
            
            if drawdown > float(Config.MAX_DRAWDOWN):
                self.logger.critical(f"EMERGENCY STOP: Drawdown {drawdown:.2%} exceeds limit {Config.MAX_DRAWDOWN:.2%}")
                
                positions_to_close = list(self.positions.keys())
                for symbol in positions_to_close:
                    self.close_position(symbol, "Emergency Drawdown Stop")
                
                self.telegram.send_message(
                    f"🚨 <b>EMERGENCY STOP ACTIVATED</b>\n"
                    f"📉 Drawdown: {drawdown:.2%}\n"
                    f"🛑 All {len(positions_to_close)} positions closed\n"
                    f"💰 Current Balance: ${float(current_balance):,.2f}\n"
                    f"📊 Peak Balance: ${float(self.equity_peak):,.2f}\n"
                    f"💔 Loss: ${float(self.equity_peak - current_balance):,.2f}",
                    critical=True
                )
                
                self.running = False
                
        except Exception as e:
            self.logger.error(f"Drawdown check failed: {e}")

    def scan_for_signals(self):
        """FIXED: Scan trading pairs with proper error handling and memory management"""
        try:
            # Process pairs in smaller batches for Railway memory limits
            batch_size = 4  # Reduced for Railway
            pairs_to_scan = Config.TRADING_PAIRS[:24]  # Limit to 24 pairs for Railway
            
            for i in range(0, len(pairs_to_scan), batch_size):
                batch = pairs_to_scan[i:i + batch_size]
                
                for symbol in batch:
                    try:
                        if symbol in self.positions:
                            continue

                        klines = self.binance.get_klines(symbol, '5m', 50)  # Reduced data size
                        if not klines or len(klines) < 20:
                            continue
                            
                        market_data = self.strategy.update_market_data(symbol, klines)
                        if not market_data:
                            continue

                        signal = self.strategy.generate_signal(symbol, market_data)

                        if signal and signal.confidence >= float(Config.SIGNAL_THRESHOLD):
                            success = self.open_position(signal)
                            if success:
                                self.logger.info(f"🎯 Signal executed: {symbol} {signal.side} (confidence: {signal.confidence:.1%})")
                                time.sleep(1)

                        time.sleep(0.5)  # Rate limiting

                    except Exception as e:
                        self.logger.warning(f"Signal scan failed for {symbol}: {e}")
                        continue
                
                time.sleep(2)  # Batch pause

        except Exception as e:
            self.logger.error(f"Signal scanning failed: {e}")

    def send_periodic_report(self):
        """Send comprehensive status report"""
        try:
            now = time.time()
            if now - self.last_report < Config.REPORT_INTERVAL:
                return

            current_balance = self.get_balance()
            positions = self.get_positions()

            total_unrealized_pnl = sum(pos['pnl'] for pos in positions)
            total_return = ((current_balance + D(str(total_unrealized_pnl)) - Config.INITIAL_BALANCE) / Config.INITIAL_BALANCE) * 100
            
            uptime_hours = (now - self.start_time) / 3600
            
            # Get trade statistics
            cursor = self.db.execute('''
                SELECT COUNT(*), 
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END),
                       AVG(pnl),
                       SUM(pnl),
                       MAX(pnl),
                       MIN(pnl),
                       AVG(duration_seconds)
                FROM trades 
                WHERE timestamp > ?
            ''', (now - 86400,))
            
            trade_stats = cursor.fetchone() if cursor else (0, 0, 0, 0, 0, 0, 0)
            daily_trades = trade_stats[0] if trade_stats[0] else 0
            daily_wins = trade_stats[1] if trade_stats[1] else 0
            avg_pnl = trade_stats[2] if trade_stats[2] else 0
            daily_pnl = trade_stats[3] if trade_stats[3] else 0
            best_trade = trade_stats[4] if trade_stats[4] else 0
            worst_trade = trade_stats[5] if trade_stats[5] else 0
            avg_duration = trade_stats[6] if trade_stats[6] else 0

            win_rate = (daily_wins / daily_trades * 100) if daily_trades > 0 else 0
            avg_duration_str = f"{avg_duration/3600:.1f}h" if avg_duration > 3600 else f"{avg_duration/60:.0f}m"

            drawdown = (self.equity_peak - current_balance) / self.equity_peak * 100 if self.equity_peak > 0 else 0

            report = (
                f"📊 <b>OMEGAX RAILWAY REPORT</b>\n"
                f"💰 Balance: ${float(current_balance):,.2f}\n"
                f"📈 Unrealized PnL: ${total_unrealized_pnl:+,.2f}\n"
                f"📊 Total Return: {float(total_return):+.2f}%\n"
                f"📉 Drawdown: {drawdown:.2f}%\n"
                f"🔢 Positions: {len(positions)}/{Config.MAX_POSITIONS}\n"
                f"⏰ Uptime: {uptime_hours:.1f}h\n\n"
                f"📈 <b>24H PERFORMANCE</b>\n"
                f"🎯 Trades: {daily_trades}\n"
                f"✅ Win Rate: {win_rate:.1f}%\n"
                f"💵 Daily PnL: ${daily_pnl:+.2f}\n"
                f"📊 Avg Trade: ${avg_pnl:+.2f}\n"
                f"🏆 Best: ${best_trade:+.2f}\n"
                f"💔 Worst: ${worst_trade:+.2f}\n"
                f"⏱️ Avg Duration: {avg_duration_str}\n\n"
                f"⚙️ <b>CONFIG</b>\n"
                f"💼 Risk/Trade: {Config.BASE_RISK_PERCENT}%\n"
                f"⚡ Leverage: {Config.LEVERAGE}x\n"
                f"🛑 Stop Loss: {Config.STOP_LOSS_PERCENT}%\n"
                f"🎯 Take Profit: {Config.TAKE_PROFIT_PERCENT}%\n"
                f"📊 Trailing TP: {Config.TRAILING_TP_PERCENT}%"
            )

            if positions:
                report += "\n\n<b>OPEN POSITIONS:</b>\n"
                for pos in positions[:3]:  # Limit to 3 for Railway
                    emoji = "🟢" if pos['pnl'] > 0 else "🔴"
                    duration = time.time() - self.positions[pos['symbol']]['timestamp']
                    duration_str = f"{duration/3600:.1f}h" if duration > 3600 else f"{duration/60:.0f}m"
                    report += f"{emoji} {pos['symbol']} {pos['side']}: ${pos['pnl']:+.2f} ({pos['percentage']:+.1f}%) [{duration_str}]\n"

            self.telegram.send_message(report)
            self.last_report = now

        except Exception as e:
            self.logger.error(f"Report generation failed: {e}")

    def run(self):
        """FIXED: Main trading loop with Railway-optimized error handling"""
        self.running = True
        self.logger.info("🚀 Starting OmegaX Railway Trading Bot")

        self.telegram.send_message(
            f"🚀 <b>OMEGAX RAILWAY BOT STARTED</b>\n"
            f"💰 Initial Balance: ${float(Config.INITIAL_BALANCE):,.2f}\n"
            f"📊 Monitoring: {len(Config.TRADING_PAIRS)} pairs\n"
            f"⚡ Leverage: {Config.LEVERAGE}x\n"
            f"🎯 Max Positions: {Config.MAX_POSITIONS}\n"
            f"💼 Risk per Trade: {Config.BASE_RISK_PERCENT}%\n"
            f"🛑 Conservative SL: {Config.STOP_LOSS_PERCENT}%\n"
            f"🎯 Conservative TP: {Config.TAKE_PROFIT_PERCENT}%\n"
            f"📊 Trailing TP: {Config.TRAILING_TP_PERCENT}%\n"
            f"🚨 Max Drawdown: {Config.MAX_DRAWDOWN:.0%}\n"
            f"🚂 RAILWAY CLOUD DEPLOYMENT",
            critical=True
        )

        consecutive_errors = 0
        max_consecutive_errors = 3  # Reduced for Railway
        error_backoff = 1

        try:
            while self.running:
                loop_start = time.time()

                try:
                    self.manage_positions()
                    self.scan_for_signals()
                    self.send_periodic_report()
                    
                    consecutive_errors = 0
                    error_backoff = 1

                except Exception as e:
                    consecutive_errors += 1
                    self.logger.error(f"Trading loop error ({consecutive_errors}/{max_consecutive_errors}): {e}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.critical("Too many consecutive errors, stopping bot")
                        self.telegram.send_message(
                            f"🚨 <b>RAILWAY BOT STOPPED - ERRORS</b>\n"
                            f"❌ Consecutive errors: {consecutive_errors}\n"
                            f"💥 Last error: {str(e)[:200]}\n"
                            f"🛑 Manual intervention required",
                            critical=True
                        )
                        break
                    
                    error_backoff = min(30, error_backoff * 2)  # Reduced for Railway
                    time.sleep(error_backoff)

                # Adaptive sleep
                loop_time = time.time() - loop_start
                sleep_time = max(3, Config.UPDATE_INTERVAL - loop_time)  # Reduced minimum
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            self.logger.info("🛑 Received shutdown signal")
        except Exception as e:
            self.logger.error(f"💥 Fatal error: {e}")
            self.telegram.send_message(f"💥 <b>FATAL ERROR</b>\n{str(e)[:200]}", critical=True)
        finally:
            self.stop()

    def stop(self):
        """FIXED: Graceful shutdown with Railway optimization"""
        self.running = False

        try:
            open_positions = list(self.positions.keys())
            if open_positions:
                self.logger.info(f"Found {len(open_positions)} open positions during shutdown")
                for symbol in open_positions:
                    pos = self.positions[symbol]
                    self.logger.info(f"Open position: {symbol} {pos['side']} {pos['size']:.6f}")

            final_balance = self.get_balance()
            total_return = ((final_balance - Config.INITIAL_BALANCE) / Config.INITIAL_BALANCE) * 100
            
            cursor = self.db.execute('''
                SELECT COUNT(*), 
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END),
                       SUM(pnl),
                       MAX(pnl),
                       MIN(pnl)
                FROM trades
            ''')
            
            stats = cursor.fetchone() if cursor else (0, 0, 0, 0, 0)
            total_trades = stats[0] if stats[0] else 0
            winning_trades = stats[1] if stats[1] else 0
            total_pnl = stats[2] if stats[2] else 0
            best_trade = stats[3] if stats[3] else 0
            worst_trade = stats[4] if stats[4] else 0
            
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
            runtime_hours = (time.time() - self.start_time) / 3600

            self.telegram.send_message(
                f"🛑 <b>OMEGAX RAILWAY BOT STOPPED</b>\n\n"
                f"💰 Final Balance: ${float(final_balance):,.2f}\n"
                f"📈 Total Return: {float(total_return):+.2f}%\n"
                f"💵 Total PnL: ${total_pnl:+.2f}\n"
                f"📊 Peak Balance: ${float(self.equity_peak):,.2f}\n\n"
                f"📊 <b>FINAL STATISTICS</b>\n"
                f"🎯 Total Trades: {total_trades}\n"
                f"✅ Win Rate: {win_rate:.1f}%\n"
                f"🏆 Best Trade: ${best_trade:+.2f}\n"
                f"💔 Worst Trade: ${worst_trade:+.2f}\n"
                f"⏰ Runtime: {runtime_hours:.1f}h\n"
                f"📊 Open Positions: {len(open_positions)}\n\n"
                f"🚂 Railway deployment completed successfully!",
                critical=True
            )
            
            self.db.close_all()
            
        except Exception as e:
            self.logger.error(f"Shutdown error: {e}")

        self.logger.info("✅ OmegaX Railway Bot stopped gracefully")

# ====================== RAILWAY HEALTH HANDLER ======================
class HealthHandler(BaseHTTPRequestHandler):
    """Railway-optimized health check handler"""
    
    def do_GET(self):
        try:
            if self.path == '/health':
                self.send_health_check()
            elif self.path == '/status':
                self.send_detailed_status()
            elif self.path == '/':
                self.send_welcome_page()
            else:
                self.send_404()
        except Exception as e:
            self.send_error_response(str(e))

    def send_health_check(self):
        """Railway health check endpoint"""
        status = {
            "status": "healthy",
            "service": "OmegaX Railway Trading Bot",
            "version": "1.0.0",
            "timestamp": time.time(),
            "environment": Config.RAILWAY_ENVIRONMENT,
            "uptime": time.time() - getattr(self.server, 'start_time', time.time())
        }

        if hasattr(self.server, 'bot') and self.server.bot:
            try:
                current_balance = self.server.bot.get_balance()
                positions = self.server.bot.get_positions()
                
                status.update({
                    "trading": {
                        "balance": float(current_balance),
                        "positions": len(positions),
                        "running": self.server.bot.running
                    }
                })
            except Exception:
                status["trading"] = {"error": "Unable to fetch trading data"}

        self.send_json_response(status, 200)

    def send_detailed_status(self):
        """Detailed bot status for Railway monitoring"""
        if not hasattr(self.server, 'bot') or not self.server.bot:
            self.send_json_response({"error": "Bot not available"}, 503)
            return

        bot = self.server.bot
        
        try:
            current_balance = bot.get_balance()
            positions = bot.get_positions()
            
            detailed_status = {
                "bot_status": {
                    "running": bot.running,
                    "uptime_hours": (time.time() - bot.start_time) / 3600,
                    "balance": float(current_balance),
                    "total_pnl": float(bot.total_pnl),
                    "equity_peak": float(bot.equity_peak)
                },
                "positions": [
                    {
                        "symbol": pos["symbol"],
                        "side": pos["side"],
                        "size": pos["size"],
                        "pnl": pos["pnl"],
                        "percentage": pos["percentage"]
                    }
                    for pos in positions
                ],
                "config": {
                    "leverage": Config.LEVERAGE,
                    "max_positions": Config.MAX_POSITIONS,
                    "risk_per_trade": f"{Config.BASE_RISK_PERCENT}%",
                    "stop_loss": f"{Config.STOP_LOSS_PERCENT}%",
                    "take_profit": f"{Config.TAKE_PROFIT_PERCENT}%"
                }
            }
            
            self.send_json_response(detailed_status, 200)
            
        except Exception as e:
            self.send_json_response({"error": str(e)}, 500)

    def send_welcome_page(self):
        """Railway welcome page"""
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>OmegaX Railway Trading Bot</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
                .container { max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; }
                .header { text-align: center; color: #333; }
                .status { background: #e8f5e8; padding: 15px; border-radius: 5px; margin: 20px 0; }
                .endpoints { background: #f0f8ff; padding: 15px; border-radius: 5px; }
                .config { background: #fff8dc; padding: 15px; border-radius: 5px; margin: 20px 0; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚀 OmegaX Railway Trading Bot</h1>
                    <p>Institutional-Grade Futures Trading System</p>
                </div>
                
                <div class="status">
                    <h3>✅ System Status</h3>
                    <p><strong>Environment:</strong> Railway Cloud</p>
                    <p><strong>Status:</strong> Active and Running</p>
                    <p><strong>Version:</strong> 1.0.0</p>
                    <p><strong>Deployment:</strong> Production Ready</p>
                </div>
                
                <div class="config">
                    <h3>⚙️ Configuration</h3>
                    <p><strong>Initial Balance:</strong> $""" + str(Config.INITIAL_BALANCE) + """</p>
                    <p><strong>Max Positions:</strong> """ + str(Config.MAX_POSITIONS) + """</p>
                    <p><strong>Leverage:</strong> """ + str(Config.LEVERAGE) + """x</p>
                    <p><strong>Risk per Trade:</strong> """ + str(Config.BASE_RISK_PERCENT) + """%</p>
                    <p><strong>Stop Loss:</strong> """ + str(Config.STOP_LOSS_PERCENT) + """%</p>
                    <p><strong>Take Profit:</strong> """ + str(Config.TAKE_PROFIT_PERCENT) + """%</p>
                </div>
                
                <div class="endpoints">
                    <h3>🔗 API Endpoints</h3>
                    <ul>
                        <li><a href="/health">/health</a> - Health check</li>
                        <li><a href="/status">/status</a> - Detailed status</li>
                        <li><a href="/">/</a> - This welcome page</li>
                    </ul>
                </div>
                
                <div class="status">
                    <h3>🛡️ Security Features</h3>
                    <ul>
                        <li>Conservative risk management</li>
                        <li>Real-time position monitoring</li>
                        <li>Automatic drawdown protection</li>
                        <li>Thread-safe operations</li>
                        <li>Comprehensive error handling</li>
                    </ul>
                </div>
            </div>
        </body>
        </html>
        """
        
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.send_header('Content-Length', str(len(html)))
        self.end_headers()
        self.wfile.write(html.encode())

    def send_404(self):
        """404 error page"""
        error = {"error": "Endpoint not found", "available_endpoints": ["/", "/health", "/status"]}
        self.send_json_response(error, 404)

    def send_error_response(self, error_msg: str):
        """Send error response"""
        error = {"error": error_msg, "timestamp": time.time()}
        self.send_json_response(error, 500)

    def send_json_response(self, data: dict, status_code: int):
        """Send JSON response with proper headers"""
        response = json.dumps(data, indent=2, default=str)
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(response)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(response.encode())

    def log_message(self, format, *args):
        return  # Suppress HTTP logs for Railway

# ====================== RAILWAY SIGNAL HANDLERS ======================
def setup_signal_handlers(bot):
    """Setup graceful shutdown for Railway"""
    def signal_handler(signum, frame):
        logging.getLogger(__name__).info(f"Received signal {signum}, initiating graceful shutdown...")
        if bot:
            bot.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

# ====================== RAILWAY MAIN ENTRY POINT ======================
def main():
    """Railway-optimized main entry point"""
    try:
        setup_logging()
        logger = logging.getLogger(__name__)

        # Railway startup banner
        print("\n" + "="*60)
        print("🚂 OMEGAX RAILWAY TRADING BOT v1.0")
        print("="*60)
        print("🏛️ Institutional-Grade Futures Trading")
        print("☁️ Railway Cloud Deployment")
        print("💎 Conservative Risk Management")
        print("🎯 High-Confidence Signal Generation")
        print("="*60)

        # Configuration validation
        use_realistic_paper = Config.USE_REALISTIC_PAPER
        if use_realistic_paper:
            logger.info("📝 REALISTIC PAPER TRADING MODE")
            logger.info("🌐 Using real Binance market data with simulated trades")
        else:
            if not Config.BINANCE_API_KEY or not Config.BINANCE_SECRET_KEY:
                logger.warning("⚠️ No Binance credentials found")
                logger.info("🔄 Automatically switching to PAPER TRADING mode")
                use_realistic_paper = True
            else:
                logger.info("💰 LIVE TRADING MODE")
                logger.warning("⚠️ REAL MONEY AT RISK - Monitor carefully!")

        if not Config.TELEGRAM_TOKEN or not Config.TELEGRAM_CHAT_ID:
            logger.warning("⚠️ Telegram credentials missing")
            logger.info("📝 Notifications will be logged to console only")

        # Display Railway-optimized configuration
        logger.info("🚂 RAILWAY CONFIGURATION")
        logger.info("="*40)
        logger.info(f"🌐 Environment: {Config.RAILWAY_ENVIRONMENT}")
        logger.info(f"🔌 Port: {Config.PORT}")
        logger.info(f"💰 Initial Balance: ${Config.INITIAL_BALANCE}")
        logger.info(f"📊 Trading Pairs: {len(Config.TRADING_PAIRS)} (Railway optimized)")
        logger.info(f"🎯 Max Positions: {Config.MAX_POSITIONS}")
        logger.info(f"💼 Risk per Trade: {Config.BASE_RISK_PERCENT}%")
        logger.info(f"⚡ Leverage: {Config.LEVERAGE}x")
        logger.info(f"🛑 Stop Loss: {Config.STOP_LOSS_PERCENT}%")
        logger.info(f"🎯 Take Profit: {Config.TAKE_PROFIT_PERCENT}%")
        logger.info(f"📊 Trailing TP: {Config.TRAILING_TP_PERCENT}%")
        logger.info(f"🚨 Max Drawdown: {Config.MAX_DRAWDOWN:.0%}")
        logger.info(f"📈 Signal Threshold: {Config.SIGNAL_THRESHOLD:.0%}")
        logger.info("="*40)

        # Initialize bot
        logger.info("🔄 Initializing OmegaX Railway Bot...")
        bot = TradingBot(use_realistic_paper=use_realistic_paper)

        # Setup signal handlers for Railway
        setup_signal_handlers(bot)

        # Start bot in separate thread
        logger.info("🚀 Starting trading engine...")
        bot_thread = threading.Thread(target=bot.run, daemon=True)
        bot_thread.start()

        # Brief startup delay
        time.sleep(2)

        # Start Railway-compatible health server
        logger.info(f"🌐 Starting Railway health server on port {Config.PORT}...")
        
        server = HTTPServer(('0.0.0.0', Config.PORT), HealthHandler)
        server.bot = bot
        server.start_time = time.time()

        logger.info("✅ OmegaX Railway Bot is now LIVE!")
        logger.info("="*40)
        logger.info("📊 RAILWAY ENDPOINTS:")
        logger.info(f"  • https://your-app.railway.app/        - Welcome page")
        logger.info(f"  • https://your-app.railway.app/health  - Health check")
        logger.info(f"  • https://your-app.railway.app/status  - Detailed status")
        logger.info("="*40)

        if use_realistic_paper:
            logger.info("🎮 PAPER TRADING MODE ACTIVE")
            logger.info("💡 Safe environment for strategy testing")
            logger.info("📊 Using real market data with simulated trades")
        else:
            logger.info("💰 LIVE TRADING MODE ACTIVE")
            logger.info("⚠️ REAL MONEY AT RISK - Monitor carefully!")

        logger.info("🚂 Railway deployment successful!")
        logger.info("="*60)

        try:
            server.serve_forever()
        except KeyboardInterrupt:
            logger.info("\n🛑 Received interrupt signal - initiating graceful shutdown...")
        finally:
            logger.info("🔄 Shutting down Railway systems...")
            bot.stop()
            server.server_close()
            logger.info("✅ OmegaX Railway Bot stopped successfully!")
            logger.info("👋 Thank you for using OmegaX Railway Trading Bot!")

    except Exception as e:
        print(f"\n💥 FATAL RAILWAY STARTUP ERROR: {e}")
        print("🔧 Please check your Railway configuration and try again")
        print("💡 Ensure all environment variables are properly set")
        sys.exit(1)

if __name__ == "__main__":
    main()