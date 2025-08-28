# -*- coding: utf-8 -*-
"""
HYBRID AI-ENHANCED BINANCE BOT (MULTI-SYMBOL, PYTHON 3.8+)
MySQL 5.6.26 PATCH: All JSON columns replaced with TEXT (MariaDB 10.4+ still OK).
Backtester: shows live progress (%) while simulating.

Install (Windows or Linux):
  python -m pip install websockets mysql-connector-python python-dotenv ccxt numpy pandas openai
"""

from __future__ import annotations
import asyncio
import json
import os
import signal
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, date
from typing import Optional, Dict, List, Tuple

import numpy as np
import pandas as pd
import websockets
from dotenv import load_dotenv

# Optional deps
try:
    import mysql.connector as mysql
except Exception:
    mysql = None

try:
    import ccxt
except Exception:
    ccxt = None

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


# -------------------------- ENV / CONFIG --------------------------
load_dotenv()

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/!miniTicker@arr"

SYMBOLS: List[str] = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]
USE_MYSQL = os.getenv("USE_MYSQL", "false").lower() == "true"
USE_OPENAI = os.getenv("USE_OPENAI", "false").lower() == "true"

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
TESTNET = os.getenv("TESTNET", "false").lower() == "true"
LIVE = os.getenv("LIVE", "false").lower() == "true"
BACKTEST = os.getenv("BACKTEST", "false").lower() == "true"

START_BALANCE_USDT = float(os.getenv("START_BALANCE_USDT", "10000"))
BASE_RISK_PCT = float(os.getenv("BASE_RISK_PCT", "0.01"))
DAILY_MAX_DRAWDOWN = float(os.getenv("DAILY_MAX_DRAWDOWN", "0.05"))
ALLOCATION_TREND_PCT = float(os.getenv("ALLOCATION_TREND_PCT", "0.6"))

MAKER_FEE_PCT = float(os.getenv("MAKER_FEE_PCT", "0.0002"))
TAKER_FEE_PCT = float(os.getenv("TAKER_FEE_PCT", "0.001"))

SLIPPAGE_PCT = float(os.getenv("SLIPPAGE_PCT", "0.0005"))

STRAT_INTERVAL_SEC = int(os.getenv("STRAT_INTERVAL_SEC", "5"))
ATR_LEN = int(os.getenv("ATR_LEN", "14"))
MA_SHORT = int(os.getenv("MA_SHORT", "20"))
MA_LONG = int(os.getenv("MA_LONG", "60"))
MOM_LOOKBACK = int(os.getenv("MOM_LOOKBACK", "30"))
VOL_WINDOW = int(os.getenv("VOL_WINDOW", "30"))
MIN_NOTIONAL = float(os.getenv("MIN_NOTIONAL", "10"))
TRAIL_PCT = float(os.getenv("TRAIL_PCT", "0.01"))

SESSION_UTC_START = int(os.getenv("SESSION_UTC_START", "0"))
SESSION_UTC_END = int(os.getenv("SESSION_UTC_END", "24"))
MAX_CONCURRENT_POS = int(os.getenv("MAX_CONCURRENT_POS", "3"))

TREND_STRENGTH_THRESH = float(os.getenv("TREND_STRENGTH_THRESH", "0.002"))
VOL_HIGH_THRESH = float(os.getenv("VOL_HIGH_THRESH", "0.01"))

AGGR_RISK_MULT = float(os.getenv("AGGR_RISK_MULT", "1.25"))
CONS_RISK_MULT = float(os.getenv("CONS_RISK_MULT", "0.75"))

def parse_symbol_caps(s: str) -> Dict[str, float]:
    caps = {}
    for part in [p.strip() for p in s.split(",") if p.strip()]:
        if ":" in part:
            sym, val = part.split(":", 1)
            try:
                caps[sym.strip().upper()] = float(val.strip())
            except Exception:
                pass
    return caps

SYMBOL_CAPS = parse_symbol_caps(os.getenv("SYMBOL_CAPS", ""))
DEFAULT_SYMBOL_CAP = float(os.getenv("DEFAULT_SYMBOL_CAP", "0.20"))

import json as _json
BASE_SLIPPAGE_PCT      = float(os.getenv("BASE_SLIPPAGE_PCT", "0.0002"))
SLIPPAGE_VOL_COEF      = float(os.getenv("SLIPPAGE_VOL_COEF", "0.10"))
SLIPPAGE_ATR_COEF      = float(os.getenv("SLIPPAGE_ATR_COEF", "0.05"))
SLIPPAGE_NOTIONAL_COEF = float(os.getenv("SLIPPAGE_NOTIONAL_COEF", "0.000002"))
SLIPPAGE_MIN_PCT       = float(os.getenv("SLIPPAGE_MIN_PCT", "0.00005"))
SLIPPAGE_MAX_PCT       = float(os.getenv("SLIPPAGE_MAX_PCT", "0.002"))
try:
    SLIPPAGE_OVERRIDES = _json.loads(os.getenv("SLIPPAGE_OVERRIDES", "{}"))
except Exception:
    SLIPPAGE_OVERRIDES = {}

AI_UPDATE_SECS = int(os.getenv("AI_UPDATE_SECS", "300"))
AI_MEMORY = os.getenv("AI_MEMORY", "true").lower() == "true"

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY") or ""
BINANCE_SECRET = os.getenv("BINANCE_SECRET") or ""
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

MYSQL_CFG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "bot"),
    "password": os.getenv("MYSQL_PASSWORD", "botpass"),
    "database": os.getenv("MYSQL_DB", "trading"),
}

BACKTEST_SYMBOLS = [s.strip() for s in os.getenv("BACKTEST_SYMBOLS", "").split(",") if s.strip()]
BACKTEST_TIMEFRAME = os.getenv("BACKTEST_TIMEFRAME", "1m")
BACKTEST_START = os.getenv("BACKTEST_START", "").strip()
BACKTEST_END = os.getenv("BACKTEST_END", "").strip()
BACKTEST_PAGE_LIMIT = int(os.getenv("BACKTEST_PAGE_LIMIT", "1000"))
BACKTEST_MAX_CALLS = int(os.getenv("BACKTEST_MAX_CALLS", "2000"))
BACKTEST_SLEEP_MS = int(os.getenv("BACKTEST_SLEEP_MS", "350"))
BACKTEST_EXPORT_DIR = os.getenv("BACKTEST_EXPORT_DIR", ".").strip()


# -------------------------- Helpers: Indicators & Metrics --------------------------
def atr_from_ohlc(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr1 = high - low
    tr2 = np.abs(high - prev_close)
    tr3 = np.abs(low - prev_close)
    tr = np.maximum(tr1, np.maximum(tr2, tr3))
    atr = pd.Series(tr).ewm(alpha=1.0 / max(1, period), adjust=False).mean().values
    return atr

def sharpe_ratio(returns: List[float], risk_free: float = 0.0, periods_per_year: int = 365 * 24 * 60) -> float:
    r = np.array(returns, dtype=float)
    if r.size == 0 or r.std(ddof=1) == 0:
        return 0.0
    return float((r.mean() - risk_free) / (r.std(ddof=1) + 1e-12) * np.sqrt(periods_per_year))

def max_drawdown(equity_curve: List[float]) -> float:
    eq = np.array(equity_curve, dtype=float)
    if eq.size == 0:
        return 0.0
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak) / (peak + 1e-12)
    return float(dd.min())

def binance_symbol_to_ccxt(market: str) -> str:
    common_quotes = ["USDT", "BUSD", "USDC", "TUSD", "FDUSD", "TRY", "EUR", "GBP", "BRL"]
    for q in sorted(common_quotes, key=len, reverse=True):
        if market.endswith(q):
            base = market[:-len(q)]
            return "%s/%s" % (base, q)
    return "%s/%s" % (market[:-4], market[-4:])

def current_session_utc(hour: Optional[int] = None) -> str:
    h = datetime.utcnow().hour if hour is None else hour
    if 0 <= h < 7:
        return "ASIA"
    if 7 <= h < 13:
        return "EU"
    if 13 <= h < 21:
        return "US"
    return "OFF"


# -------------------------- Dynamic Slippage Helper --------------------------
def dynamic_slippage_pct(symbol_key: str,
                         notional: float,
                         vol_now: float,
                         vol_avg: float,
                         atr_abs: float,
                         last_price: float) -> float:
    key = (symbol_key or "").upper()
    if key in SLIPPAGE_OVERRIDES:
        s = float(SLIPPAGE_OVERRIDES[key])
        return min(max(s, SLIPPAGE_MIN_PCT), SLIPPAGE_MAX_PCT)

    s = BASE_SLIPPAGE_PCT
    if vol_avg > 0:
        liq_factor = max(0.0, 1.0 - (vol_now / vol_avg))
        s += SLIPPAGE_VOL_COEF * liq_factor * BASE_SLIPPAGE_PCT
    if last_price > 0:
        volat = atr_abs / last_price
        s += SLIPPAGE_ATR_COEF * volat
    s += SLIPPAGE_NOTIONAL_COEF * max(0.0, notional)
    s = min(max(s, SLIPPAGE_MIN_PCT), SLIPPAGE_MAX_PCT)
    return s


# -------------------------- MySQL Client (MySQL 5.6.26 SAFE) --------------------------
class MySQLClient(object):
    def __init__(self, cfg: Dict[str, object]):
        if mysql is None:
            raise RuntimeError("mysql-connector-python not installed.")
        self.cfg = cfg
        self.conn = None

    def connect(self) -> None:
        self.conn = mysql.connect(**self.cfg)
        self.conn.autocommit = True
        self.ensure_tables()

    def ensure_tables(self) -> None:
        cur = self.conn.cursor()
        # live run tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ticks (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                event_time BIGINT NOT NULL,
                price DECIMAL(24,10) NOT NULL,
                vol DECIMAL(24,10) NULL,
                recv_ts BIGINT NOT NULL,
                KEY idx_symbol_time (symbol, event_time)
            ) ENGINE=InnoDB;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ts_ms BIGINT NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                action VARCHAR(16) NOT NULL,
                reason VARCHAR(16) NOT NULL,
                price DECIMAL(24,10) NULL,
                atr DECIMAL(24,10) NULL,
                mom DECIMAL(24,10) NULL,
                ma_s DECIMAL(24,10) NULL,
                ma_l DECIMAL(24,10) NULL,
                vol_now DECIMAL(24,10) NULL,
                vol_avg DECIMAL(24,10) NULL,
                KEY idx_sig_symbol_time (symbol, ts_ms)
            ) ENGINE=InnoDB;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ts_ms BIGINT NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                side VARCHAR(8) NOT NULL,
                qty DECIMAL(28,12) NOT NULL,
                price DECIMAL(24,10) NOT NULL,
                fee DECIMAL(24,10) NOT NULL,
                pnl DECIMAL(24,10) NOT NULL,
                reason VARCHAR(32) NOT NULL,
                KEY idx_trd_symbol_time (symbol, ts_ms)
            ) ENGINE=InnoDB;
        """)
        # MySQL 5.6.26: JSON -> TEXT
        cur.execute("""
            CREATE TABLE IF NOT EXISTS equity (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ts_ms BIGINT NOT NULL,
                balance_usdt DECIMAL(24,10) NOT NULL,
                realized_pnl DECIMAL(24,10) NOT NULL,
                positions_json TEXT NULL
            ) ENGINE=InnoDB;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ai_params (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                day_date DATE NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                mode VARCHAR(16) NULL,
                params_json TEXT NOT NULL,
                session_json TEXT NULL,
                perf_json TEXT NULL,
                created_ts BIGINT NOT NULL,
                UNIQUE KEY uq_day_symbol (day_date, symbol)
            ) ENGINE=InnoDB;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS backtest_runs (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(64) NOT NULL,
                started_at BIGINT NOT NULL,
                ended_at BIGINT NOT NULL,
                note VARCHAR(255) NULL,
                UNIQUE KEY uq_run (run_id)
            ) ENGINE=InnoDB;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS backtest_ohlcv (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(64) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                ts_ms BIGINT NOT NULL,
                open DECIMAL(24,10) NOT NULL,
                high DECIMAL(24,10) NOT NULL,
                low DECIMAL(24,10) NOT NULL,
                close DECIMAL(24,10) NOT NULL,
                vol DECIMAL(24,10) NOT NULL,
                KEY idx_bto (run_id, symbol, ts_ms)
            ) ENGINE=InnoDB;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS backtest_trades (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(64) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                ts_ms BIGINT NOT NULL,
                side VARCHAR(8) NOT NULL,
                price DECIMAL(24,10) NOT NULL,
                qty DECIMAL(28,12) NOT NULL,
                pnl_quote DECIMAL(24,10) NOT NULL,
                event VARCHAR(32) NOT NULL,
                slippage_pct DECIMAL(12,8) NULL,
                KEY idx_btt (run_id, symbol, ts_ms)
            ) ENGINE=InnoDB;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS backtest_equity (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(64) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                ts_ms BIGINT NOT NULL,
                equity DECIMAL(24,10) NOT NULL,
                KEY idx_bte (run_id, symbol, ts_ms)
            ) ENGINE=InnoDB;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS backtest_summary (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(64) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                trades INT NOT NULL,
                win_rate DECIMAL(12,8) NOT NULL,
                max_drawdown DECIMAL(12,8) NOT NULL,
                sharpe DECIMAL(12,8) NOT NULL,
                final_balance DECIMAL(24,10) NOT NULL,
                start_balance DECIMAL(24,10) NOT NULL,
                export_csv VARCHAR(255) NULL,
                KEY idx_bts (run_id, symbol)
            ) ENGINE=InnoDB;
        """)
        cur.close()

    # --- live inserts ---
    def insert_tick(self, symbol: str, event_time: int, price: float, vol: float, recv_ts: int) -> None:
        cur = self.conn.cursor()
        cur.execute("INSERT INTO ticks (symbol, event_time, price, vol, recv_ts) VALUES (%s,%s,%s,%s,%s)",
                    (symbol, event_time, price, vol, recv_ts))
        cur.close()

    def insert_signal(self, ts_ms: int, symbol: str, action: str, reason: str,
                      price: float, atr: float, mom: float, ma_s: float, ma_l: float,
                      vol_now: float, vol_avg: float) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO signals (ts_ms, symbol, action, reason, price, atr, mom, ma_s, ma_l, vol_now, vol_avg)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (ts_ms, symbol, action, reason, price, atr, mom, ma_s, ma_l, vol_now, vol_avg))
        cur.close()

    def insert_trade(self, ts_ms: int, symbol: str, side: str, qty: float,
                     price: float, fee: float, pnl: float, reason: str) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO trades (ts_ms, symbol, side, qty, price, fee, pnl, reason)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (ts_ms, symbol, side, qty, price, fee, pnl, reason))
        cur.close()

    def insert_equity(self, ts_ms: int, balance_usdt: float, realized_pnl: float, positions_json: str) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO equity (ts_ms, balance_usdt, realized_pnl, positions_json)
            VALUES (%s,%s,%s,%s)
        """, (ts_ms, balance_usdt, realized_pnl, positions_json))
        cur.close()

    # --- AI memory ---
    def upsert_ai_params(self, day_date: date, symbol: str, mode: str,
                         params_json: dict, session_json: dict, perf_json: dict) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO ai_params (day_date, symbol, mode, params_json, session_json, perf_json, created_ts)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE mode=VALUES(mode), params_json=VALUES(params_json),
            session_json=VALUES(session_json), perf_json=VALUES(perf_json), created_ts=VALUES(created_ts)
        """, (day_date, symbol, mode, json.dumps(params_json), json.dumps(session_json), json.dumps(perf_json),
              int(time.time()*1000)))
        cur.close()

    # --- backtest inserts ---
    def insert_backtest_run(self, run_id: str, started_at: int, ended_at: int, note: str = "") -> None:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT IGNORE INTO backtest_runs (run_id, started_at, ended_at, note)
            VALUES (%s,%s,%s,%s)
        """, (run_id, started_at, ended_at, note))
        cur.close()

    def insert_ohlcv_rows(self, run_id: str, symbol: str, df: pd.DataFrame) -> None:
        cur = self.conn.cursor()
        rows = df[["time","open","high","low","close","vol"]].values.tolist()
        for r in rows:
            cur.execute("""
                INSERT INTO backtest_ohlcv (run_id, symbol, ts_ms, open, high, low, close, vol)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (run_id, symbol, int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
        cur.close()

    def insert_backtest_trade(self, run_id: str, symbol: str, ts_ms: int, side: str,
                              price: float, qty: float, pnl_quote: float, event: str, slippage_pct: float):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO backtest_trades (run_id, symbol, ts_ms, side, price, qty, pnl_quote, event, slippage_pct)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, symbol, ts_ms, side, price, qty, pnl_quote, event, slippage_pct))
        cur.close()

    def insert_backtest_equity(self, run_id: str, symbol: str, ts_ms: int, equity: float):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO backtest_equity (run_id, symbol, ts_ms, equity)
            VALUES (%s,%s,%s,%s)
        """, (run_id, symbol, ts_ms, equity))
        cur.close()

    def insert_backtest_summary(self, run_id: str, symbol: str, trades: int, win_rate: float,
                                max_drawdown: float, sharpe: float, final_balance: float,
                                start_balance: float, export_csv: str):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO backtest_summary (run_id, symbol, trades, win_rate, max_drawdown, sharpe,
                                          final_balance, start_balance, export_csv)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, symbol, trades, win_rate, max_drawdown, sharpe, final_balance, start_balance, export_csv))
        cur.close()


# -------------------------- Exchange (ccxt) --------------------------
class Exchange(object):
    def __init__(self):
        if ccxt is None:
            raise RuntimeError("ccxt not installed.")
        self.exchange = ccxt.binance({
            'apiKey': BINANCE_API_KEY,
            'secret': BINANCE_SECRET,
            'enableRateLimit': True,
        })
        if TESTNET:
            self.exchange.set_sandbox_mode(True)
        try:
            self.exchange.load_markets()
        except Exception:
            pass
        self.markets = getattr(self.exchange, "markets", {})

    def market_symbol(self, s: str) -> str:
        return binance_symbol_to_ccxt(s)

    def fetch_ohlcv(self, mkt_symbol: str, timeframe: str = "1m", since=None, limit: int = 500):
        return self.exchange.fetch_ohlcv(mkt_symbol, timeframe=timeframe, since=since, limit=limit)

    def amount_to_precision(self, symbol_ccxt: str, amount: float) -> float:
        try:
            return float(self.exchange.amount_to_precision(symbol_ccxt, amount))
        except Exception:
            return float(round(amount, 6))

    def price_to_precision(self, symbol_ccxt: str, price: float) -> float:
        try:
            return float(self.exchange.price_to_precision(symbol_ccxt, price))
        except Exception:
            return float(round(price, 2))

    def min_notional(self, symbol_ccxt: str) -> float:
        m = self.markets.get(symbol_ccxt, {})
        lims = m.get('limits', {})
        cost = lims.get('cost', {})
        mn = cost.get('min')
        return float(mn) if mn is not None else 5.0

    def meets_min_notional(self, symbol_ccxt: str, qty: float, price: float) -> bool:
        return (qty * price) >= self.min_notional(symbol_ccxt)

    def create_order(self, symbol_ccxt: str, side: str, qty: float, price: Optional[float] = None, type_: str = 'market'):
        if type_ == 'market':
            return self.exchange.create_order(symbol_ccxt, 'market', side, qty)
        else:
            return self.exchange.create_order(symbol_ccxt, 'limit', side, qty, price)


# -------------------------- WebSocket Manager --------------------------
class WSManager(object):
    def __init__(self, url: str, symbols_filter: List[str]):
        self.url = url
        self.ws = None
        self.symbols = set(symbols_filter)
        self.price_cache: Dict[str, float] = {}
        self.tick_buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=6000))
        self._stop = asyncio.Event()

    async def connect(self):
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(self.url, ping_interval=None, max_queue=4096) as ws:
                    self.ws = ws
                    backoff = 1.0
                    await self._run()
            except Exception as e:
                print("[WS] Disconnected: %s. Reconnecting in %.1fs..." % (e, backoff))
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2.0)

    async def _run(self):
        last_ping_ts = time.time()
        while not self._stop.is_set():
            try:
                if time.time() - last_ping_ts > 120:
                    await self._send_ping()
                    last_ping_ts = time.time()

                msg = await asyncio.wait_for(self.ws.recv(), timeout=180)
                recv_ts = int(time.time() * 1000)
                data = json.loads(msg)

                if isinstance(data, list):  # miniTicker@arr
                    for item in data:
                        sym = item.get('s')
                        if self.symbols and sym not in self.symbols:
                            continue
                        price = float(item.get('c'))
                        vol = float(item.get('v', 0.0))
                        self.price_cache[sym] = price
                        self.tick_buffers[sym].append((recv_ts, price, vol))
                elif isinstance(data, dict) and 'ping' in data:
                    await self._send_pong(data['ping'])

            except asyncio.TimeoutError:
                await self._send_ping()
                last_ping_ts = time.time()
            except websockets.ConnectionClosed as e:
                print("[WS] Connection closed: %s" % e)
                break

    async def _send_ping(self):
        try:
            await self.ws.ping()
        except Exception as e:
            print("[WS] ping failed: %s" % e)

    async def _send_pong(self, payload):
        try:
            await self.ws.pong(payload)
        except Exception as e:
            print("[WS] pong failed: %s" % e)

    async def stop(self):
        self._stop.set()
        try:
            if self.ws:
                await asyncio.wait_for(self.ws.close(), timeout=5)
        except Exception:
            pass


# -------------------------- Data Classes --------------------------
@dataclass
class Position:
    symbol: str
    qty: float = 0.0
    avg_price: float = 0.0
    stop_price: float = 0.0
    take_profit: float = 0.0
    trail_pct: float = TRAIL_PCT
    highest_price: float = 0.0

    def update_trailing(self, last_price: float) -> None:
        if self.qty <= 0:
            return
        self.highest_price = max(self.highest_price, last_price)
        self.stop_price = max(self.stop_price, self.highest_price * (1.0 - self.trail_pct))


@dataclass
class Account:
    balance_usdt: float = START_BALANCE_USDT
    positions: Dict[str, Position] = field(default_factory=dict)
    realized_pnl: float = 0.0


# -------------------------- Risk Manager --------------------------
class RiskManager(object):
    def __init__(self, base_risk_pct: float = BASE_RISK_PCT, daily_dd: float = DAILY_MAX_DRAWDOWN):
        self.base_risk_pct = base_risk_pct
        self.daily_dd = daily_dd
        self.day_start_equity = START_BALANCE_USDT
        self.day = datetime.now(timezone.utc).date()

    def reset_day_if_needed(self, account: Account) -> None:
        utc_today = datetime.now(timezone.utc).date()
        if utc_today != self.day:
            self.day = utc_today
            self.day_start_equity = account.balance_usdt + account.realized_pnl

    def allow_trading(self, account: Account) -> bool:
        self.reset_day_if_needed(account)
        equity = account.balance_usdt + account.realized_pnl
        dd = (equity - self.day_start_equity) / (self.day_start_equity + 1e-12)
        return dd >= -self.daily_dd

    def symbol_cap_fraction(self, symbol: str) -> float:
        if symbol.upper() in SYMBOL_CAPS:
            return max(0.0, min(1.0, SYMBOL_CAPS[symbol.upper()]))
        if "DEFAULT" in SYMBOL_CAPS:
            return max(0.0, min(1.0, SYMBOL_CAPS["DEFAULT"]))
        return max(0.0, min(1.0, DEFAULT_SYMBOL_CAP))

    def symbol_cap_usdt(self, account: 'Account', symbol: str) -> float:
        equity = account.balance_usdt + account.realized_pnl
        return equity * self.symbol_cap_fraction(symbol)

    def position_size_usdt(self, account: 'Account', entry: float, stop: float,
                           symbol: str = "", mode_mult: float = 1.0) -> float:
        risk_amt = account.balance_usdt * self.base_risk_pct * ALLOCATION_TREND_PCT * mode_mult
        risk_per_unit = abs(entry - stop)
        if risk_per_unit <= 0:
            return 0.0
        qty = risk_amt / risk_per_unit
        notional = qty * entry
        global_cap = account.balance_usdt * ALLOCATION_TREND_PCT
        notional = min(notional, global_cap)
        if symbol:
            per_symbol_cap = self.symbol_cap_usdt(account, symbol)
            notional = min(notional, per_symbol_cap)
        return notional


# -------------------------- Strategy --------------------------
class Strategy(object):
    """
    Multi-symbol trend/vol strategy with AI overlays:
      - Regime detection -> aggressive/conservative mode
      - Per-symbol AI tuning of params + session-aware overrides
      - AI memory persisted daily per symbol (+ recent perf deltas)
    """
    def __init__(self, account: Account, risk: RiskManager, ws: WSManager,
                 db: Optional[MySQLClient] = None, exchange: Optional[Exchange] = None):
        self.account = account
        self.risk = risk
        self.ws = ws
        self.db = db
        self.ex = exchange

        self.klines: Dict[str, pd.DataFrame] = {}
        self.default_params = {
            "atr_mult_stop": 1.8,
            "atr_mult_tp": 2.6,
            "min_vol_factor": 0.7,
            "mom_thresh": 0.001,
        }
        self.symbol_params: Dict[str, Dict[str, float]] = defaultdict(lambda: self.default_params.copy())
        self.session_overrides: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.symbol_mode: Dict[str, str] = defaultdict(lambda: "auto")
        self.metrics = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0, "returns": []})
        self.last_ai_ts = 0.0

    def in_session(self) -> bool:
        h = datetime.utcnow().hour
        return SESSION_UTC_START <= h < SESSION_UTC_END

    async def refresh_klines(self) -> None:
        if self.ex is None:
            return
        for s in SYMBOLS:
            try:
                mkt = self.ex.market_symbol(s)
                limit = max(MA_LONG, ATR_LEN, VOL_WINDOW) + 5
                data = self.ex.fetch_ohlcv(mkt, timeframe="1m", since=None, limit=limit)
                self.klines[s] = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "vol"])
            except Exception as e:
                print("[KLN] fetch_ohlcv failed %s: %s" % (s, e))

    def indicators(self, symbol: str) -> Optional[Dict[str, float]]:
        df = self.klines.get(symbol)
        if df is None or len(df) < max(MA_LONG, ATR_LEN, VOL_WINDOW) + 1:
            return None
        close = df["close"].values.astype(float)
        high = df["high"].values.astype(float)
        low = df["low"].values.astype(float)
        vol = df["vol"].values.astype(float)

        atr = atr_from_ohlc(high, low, close, ATR_LEN)
        ma_s = pd.Series(close).rolling(MA_SHORT).mean().values
        ma_l = pd.Series(close).rolling(MA_LONG).mean().values

        if len(close) < MOM_LOOKBACK + 1:
            return None
        mom = (close[-1] - close[-MOM_LOOKBACK]) / (close[-MOM_LOOKBACK] + 1e-12)
        vol_now = vol[-1]
        vol_avg = vol[-VOL_WINDOW:].mean()
        last = close[-1]

        trend_strength = (ma_s[-1] - ma_l[-1]) / (ma_l[-1] + 1e-12)
        vol_ratio = (atr[-1] / last) if last > 0 else 0.0
        trend = "up" if trend_strength > TREND_STRENGTH_THRESH else ("down" if trend_strength < -TREND_STRENGTH_THRESH else "flat")
        vol_regime = "high" if vol_ratio > VOL_HIGH_THRESH else "low"

        return {
            "last": float(last),
            "atr": float(atr[-1]),
            "ma_s": float(ma_s[-1]),
            "ma_l": float(ma_l[-1]),
            "mom": float(mom),
            "vol_now": float(vol_now),
            "vol_avg": float(vol_avg),
            "trend_strength": float(trend_strength),
            "vol_ratio": float(vol_ratio),
            "trend": trend,
            "vol_regime": vol_regime
        }

    def apply_session_overrides(self, symbol: str, base: Dict[str, float]) -> Dict[str, float]:
        sess = current_session_utc()
        overrides = self.session_overrides.get(symbol, {}).get(sess, {})
        out = base.copy()
        for k in ["atr_mult_stop", "atr_mult_tp", "min_vol_factor", "mom_thresh"]:
            if k in overrides:
                out[k] = float(overrides[k])
        return out

    def effective_mode_mult(self, symbol: str, derived_mode: str) -> float:
        mode = self.symbol_mode.get(symbol, "auto")
        if mode == "aggressive":
            return AGGR_RISK_MULT
        if mode == "conservative":
            return CONS_RISK_MULT
        return AGGR_RISK_MULT if derived_mode == "aggressive" else CONS_RISK_MULT

    def derived_mode_from_regime(self, ind: Dict[str, float]) -> str:
        bullish = (ind["ma_s"] > ind["ma_l"]) and (ind["mom"] > self.symbol_params_default("mom_thresh"))
        strong_trend = abs(ind["trend_strength"]) > TREND_STRENGTH_THRESH and ind["trend"] != "flat"
        liquid = (ind["vol_avg"] > 0 and (ind["vol_now"] / ind["vol_avg"]) >= self.symbol_params_default("min_vol_factor"))
        if strong_trend and liquid and (ind["vol_regime"] == "high" or bullish):
            return "aggressive"
        return "conservative"

    def symbol_params_default(self, key: str) -> float:
        return float(self.default_params.get(key))

    async def ai_advise(self):
        if not USE_OPENAI or OpenAI is None or not OPENAI_API_KEY:
            return
        if time.time() - self.last_ai_ts < AI_UPDATE_SECS:
            return

        snapshot = {}
        for s in SYMBOLS:
            ind = self.indicators(s)
            if not ind:
                continue
            m = self.metrics[s]
            trades = int(m["trades"]); wins = int(m["wins"])
            win_rate = (wins / trades) if trades > 0 else 0.0
            pnl = float(m["pnl"])
            sess = current_session_utc()
            derived_mode = self.derived_mode_from_regime(ind)
            snapshot[s] = {
                "ind": ind,
                "recent_perf": {"trades": trades, "win_rate": round(win_rate,4), "pnl_quote": round(pnl,4)},
                "session": sess,
                "derived_mode": derived_mode,
                "current_params": self.symbol_params.get(s, self.default_params.copy())
            }

        if not snapshot:
            return

        prompt = (
            "Return ONLY JSON mapping symbol->config. Keys: "
            "{\"SYMBOL\":{\"atr_mult_stop\":1.0..3.0,\"atr_mult_tp\":1.5..4.0,"
            "\"min_vol_factor\":0.5..1.5,\"mom_thresh\":0.000..0.010,"
            "\"mode\":\"aggressive|conservative|auto\","
            "\"session_overrides\": {\"ASIA\":{...},\"EU\":{...},\"US\":{...},\"OFF\":{...}}}}. "
            "Nudge slightly from current_params, respect ranges. Consider recent_perf & indicators. "
            "Input:" + json.dumps(snapshot)[:6000]
        )

        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            try:
                resp = client.chat_completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "Return ONLY strict JSON. No prose."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.2,
                )
                txt = resp.choices[0].message.content.strip()
            except Exception:
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "Return ONLY strict JSON. No prose."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.2,
                )
                txt = resp.choices[0].message.content.strip()
        except Exception as e:
            print("[AI] advisory error:", e)
            return

        try:
            j = json.loads(txt)
        except Exception:
            j = {}

        def clamp(v, lo, hi, default):
            try:
                vv = float(v)
                if vv < lo: vv = lo
                if vv > hi: vv = hi
                return vv
            except Exception:
                return default

        for s, cfg in j.items():
            if s not in SYMBOLS:
                continue
            cur = self.symbol_params.get(s, self.default_params.copy())
            new_params = {
                "atr_mult_stop": clamp(cfg.get("atr_mult_stop"), 1.0, 3.0, cur.get("atr_mult_stop", 1.8)),
                "atr_mult_tp":   clamp(cfg.get("atr_mult_tp"),   1.5, 4.0, cur.get("atr_mult_tp", 2.6)),
                "min_vol_factor":clamp(cfg.get("min_vol_factor"),0.5, 1.5, cur.get("min_vol_factor", 0.7)),
                "mom_thresh":    clamp(cfg.get("mom_thresh"),    0.0, 0.010, cur.get("mom_thresh", 0.001)),
            }
            self.symbol_params[s] = new_params

            mode = cfg.get("mode", "auto")
            if mode not in ["aggressive", "conservative", "auto"]:
                mode = "auto"
            self.symbol_mode[s] = mode

            sess_over = {}
            so = cfg.get("session_overrides", {})
            for sess_key in ["ASIA","EU","US","OFF"]:
                if sess_key in so and isinstance(so[sess_key], dict):
                    sess_over[sess_key] = {}
                    for k in ["atr_mult_stop","atr_mult_tp","min_vol_factor","mom_thresh"]:
                        if k in so[sess_key]:
                            if k == "atr_mult_stop":
                                sess_over[sess_key][k] = clamp(so[sess_key][k], 1.0, 3.0, new_params["atr_mult_stop"])
                            elif k == "atr_mult_tp":
                                sess_over[sess_key][k] = clamp(so[sess_key][k], 1.5, 4.0, new_params["atr_mult_tp"])
                            elif k == "min_vol_factor":
                                sess_over[sess_key][k] = clamp(so[sess_key][k], 0.5, 1.5, new_params["min_vol_factor"])
                            elif k == "mom_thresh":
                                sess_over[sess_key][k] = clamp(so[sess_key][k], 0.0, 0.010, new_params["mom_thresh"])
            self.session_overrides[s] = sess_over

            if self.db and AI_MEMORY:
                m = self.metrics[s]
                perf = {"trades": int(m["trades"]), "wins": int(m["wins"]),
                        "win_rate": (float(m["wins"])/max(1,int(m["trades"]))), "pnl_quote": float(m["pnl"])}
                try:
                    self.db.upsert_ai_params(datetime.utcnow().date(), s, mode, new_params, sess_over, perf)
                except Exception as e:
                    print("[AI][DB] upsert failed:", e)

        self.last_ai_ts = time.time()
        print("[AI] Updated per-symbol params:", self.symbol_params)
        print("[AI] Modes:", dict(self.symbol_mode))
        if any(self.session_overrides.values()):
            print("[AI] Session overrides present.")

    def log_signal(self, symbol: str, action: str, reason: str, ind: Dict[str, float]) -> None:
        if not self.db:
            return
        ts = int(time.time() * 1000)
        try:
            self.db.insert_signal(
                ts, symbol, action, reason,
                float(ind.get("last", 0.0)), float(ind.get("atr", 0.0)), float(ind.get("mom", 0.0)),
                float(ind.get("ma_s", 0.0)), float(ind.get("ma_l", 0.0)),
                float(ind.get("vol_now", 0.0)), float(ind.get("vol_avg", 0.0))
            )
        except Exception as e:
            print("[DB] signal insert failed:", e)

    def log_trade(self, symbol: str, side: str, qty: float, price: float, fee: float, pnl: float, reason: str) -> None:
        if self.db:
            ts = int(time.time() * 1000)
            try:
                self.db.insert_trade(ts, symbol, side, qty, price, fee, pnl, reason)
            except Exception as e:
                print("[DB] trade insert failed:", e)
        m = self.metrics[symbol]
        if side.lower() == "sell":
            m["trades"] += 1
            if pnl > 0:
                m["wins"] += 1
            denom = max(1e-9, qty * price)
            m["returns"].append(pnl / denom)
            m["pnl"] += pnl

    def log_equity(self) -> None:
        if not self.db:
            return
        ts = int(time.time() * 1000)
        pos_dict = {k: vars(v) for k, v in self.account.positions.items()}
        try:
            self.db.insert_equity(ts, float(self.account.balance_usdt), float(self.account.realized_pnl), json.dumps(pos_dict))
        except Exception as e:
            print("[DB] equity insert failed:", e)

    async def maybe_log_tick(self) -> None:
        if not self.db:
            return
        now_ms = int(time.time() * 1000)
        for sym, price in list(self.ws.price_cache.items()):
            buf = self.ws.tick_buffers[sym]
            if not buf:
                continue
            _, p, v = buf[-1]
            try:
                self.db.insert_tick(sym, now_ms, float(p), float(v), now_ms)
            except Exception as e:
                print("[DB] tick insert failed:", e)

    def compute_stop_tp(self, price: float, atr: float, symbol: str) -> Tuple[float, float]:
        base = self.symbol_params.get(symbol, self.default_params)
        base = self.apply_session_overrides(symbol, base)
        stop = price - base["atr_mult_stop"] * atr
        tp = price + base["atr_mult_tp"] * atr
        return max(0.0, stop), max(price, tp)

    async def open_position(self, symbol: str, qty: float, price: float, stop: float, tp: float,
                            slip_pct: Optional[float] = None) -> None:
        slip = slip_pct if (slip_pct is not None) else SLIPPAGE_PCT
        eff_price = price * (1.0 + slip)
        cost = qty * eff_price
        fee = cost * TAKER_FEE_PCT
        eff_cost = cost + fee

        pos = Position(symbol=symbol, qty=qty, avg_price=eff_price,
                       stop_price=stop, take_profit=tp, highest_price=eff_price)
        self.account.positions[symbol] = pos

        if DRY_RUN or not LIVE:
            self.account.balance_usdt -= eff_cost
            self.log_trade(symbol, "buy", qty, eff_price, fee, 0.0, "open")
            print("[OPEN]", symbol, "qty=", qty, "@", round(eff_price, 4),
                  "fee=", round(fee, 6), "slip=", round(slip*10000, 3), "bp",
                  "bal=", round(self.account.balance_usdt, 2))
        else:
            try:
                ord_symbol = self.ex.market_symbol(symbol)
                qty_p = self.ex.amount_to_precision(ord_symbol, qty)
                if not self.ex.meets_min_notional(ord_symbol, qty_p, price):
                    print("[LIVE OPEN] below min notional; skip", symbol); return
                order = self.ex.create_order(ord_symbol, 'buy', qty_p, None, 'market')
                print("[LIVE OPEN]", order)
            except Exception as e:
                print("[LIVE OPEN ERROR]", e)

    async def close_position(self, symbol: str, price: float, reason: str, slip_pct: Optional[float] = None) -> None:
        pos = self.account.positions.get(symbol)
        if not pos or pos.qty <= 0:
            return
        slip = slip_pct if (slip_pct is not None) else SLIPPAGE_PCT
        eff_price = price * (1.0 - slip)
        proceeds = pos.qty * eff_price
        fee = proceeds * TAKER_FEE_PCT
        net = proceeds - fee
        pnl = net - (pos.qty * pos.avg_price)

        self.account.realized_pnl += pnl
        self.account.balance_usdt += net
        self.log_trade(symbol, "sell", pos.qty, eff_price, fee, pnl, "close-" + reason)
        print("[CLOSE-%s]" % reason, symbol, "qty=", pos.qty, "@", round(eff_price, 4),
              "pnl=", round(pnl, 4), "slip=", round(slip*10000,3), "bp",
              "bal=", round(self.account.balance_usdt, 2))

        if LIVE and not DRY_RUN:
            try:
                ord_symbol = self.ex.market_symbol(symbol)
                qty_p = self.ex.amount_to_precision(ord_symbol, pos.qty)
                if not self.ex.meets_min_notional(ord_symbol, qty_p, price):
                    print("[LIVE CLOSE] below min notional; skip", symbol)
                else:
                    order = self.ex.create_order(ord_symbol, 'sell', qty_p, None, 'market')
                    print("[LIVE CLOSE]", order)
            except Exception as e:
                print("[LIVE CLOSE ERROR]", e)

        self.account.positions.pop(symbol, None)

    async def step(self) -> None:
        if not self.in_session():
            return
        if not self.risk.allow_trading(self.account):
            return

        await self.ai_advise()

        for symbol in SYMBOLS:
            ind = self.indicators(symbol)
            if not ind:
                continue

            base = self.symbol_params.get(symbol, self.default_params)
            eff_params = self.apply_session_overrides(symbol, base)

            last = ind["last"]; atr = ind["atr"]
            ma_s = ind["ma_s"]; ma_l = ind["ma_l"]
            mom = ind["mom"]; vol_now = ind["vol_now"]; vol_avg = ind["vol_avg"]

            if vol_avg <= 0 or (vol_now / vol_avg) < eff_params["min_vol_factor"]:
                continue

            bullish = (ma_s > ma_l) and (mom > eff_params["mom_thresh"])
            bearish = (ma_s < ma_l) and (mom < -eff_params["mom_thresh"])

            pos = self.account.positions.get(symbol, Position(symbol))

            derived_mode = self.derived_mode_from_regime(ind)
            mode_mult = self.effective_mode_mult(symbol, derived_mode)

            if pos.qty > 0:
                pos.update_trailing(last)
                if last <= pos.stop_price and pos.stop_price > 0.0:
                    sell_notional = pos.qty * last
                    slip = dynamic_slippage_pct(symbol, sell_notional, vol_now, vol_avg, atr, last)
                    self.log_signal(symbol, "close", "stop", ind)
                    await self.close_position(symbol, last, "stop", slip_pct=slip)
                    continue
                if last >= pos.take_profit and pos.take_profit > 0.0:
                    sell_notional = pos.qty * last
                    slip = dynamic_slippage_pct(symbol, sell_notional, vol_now, vol_avg, atr, last)
                    self.log_signal(symbol, "close", "tp", ind)
                    await self.close_position(symbol, last, "tp", slip_pct=slip)
                    continue

            if pos.qty == 0 and bullish and len(self.account.positions) < MAX_CONCURRENT_POS:
                stop, tp = self.compute_stop_tp(last, atr, symbol)
                notional = self.risk.position_size_usdt(self.account, last, stop, symbol, mode_mult=mode_mult)
                if notional > MIN_NOTIONAL:
                    qty = round(notional / last, 6)
                    slip = dynamic_slippage_pct(symbol, notional, vol_now, vol_avg, atr, last)
                    self.log_signal(symbol, "open", "bullish", ind)
                    await self.open_position(symbol, qty, last, stop, tp, slip_pct=slip)

            if pos.qty > 0 and bearish:
                sell_notional = pos.qty * last
                slip = dynamic_slippage_pct(symbol, sell_notional, vol_now, vol_avg, atr, last)
                self.log_signal(symbol, "close", "flip", ind)
                await self.close_position(symbol, last, "flip", slip_pct=slip)

        self.log_equity()


# -------------------------- Backtester (progress %) --------------------------
def fetch_ohlcv_paginated(ccxt_ex, symbol_ccxt: str, timeframe="1m",
                          start_iso="", end_iso="", limit_per_call=1000,
                          max_calls=2000, extra_sleep_ms=350):
    since = int(pd.Timestamp(start_iso).timestamp() * 1000) if start_iso else None
    end_ms = int(pd.Timestamp(end_iso).timestamp() * 1000) if end_iso else None
    all_rows = []
    calls = 0
    rate_limit_ms = getattr(ccxt_ex, "rateLimit", 250)

    while True:
        calls += 1
        if calls > max_calls:
            break
        batch = ccxt_ex.fetch_ohlcv(symbol_ccxt, timeframe=timeframe, since=since, limit=limit_per_call)
        if not batch:
            break
        all_rows.extend(batch)
        last_time = batch[-1][0]
        next_since = last_time + 1
        if end_ms and next_since >= end_ms:
            break
        if len(batch) < limit_per_call:
            break
        since = next_since
        slp = (rate_limit_ms + extra_sleep_ms) / 1000.0
        time.sleep(slp)

    df = pd.DataFrame(all_rows, columns=["time","open","high","low","close","vol"]).drop_duplicates(subset=["time"])
    if end_ms:
        df = df[df["time"] <= end_ms].copy()
    df.reset_index(drop=True, inplace=True)
    return df


class Backtester(object):
    def __init__(self, exchange: Exchange, db: Optional[MySQLClient] = None):
        self.ex = exchange
        self.db = db

    def run_symbol(self, run_id: str, mkt_symbol: str, timeframe: str="1m",
                   start_iso: str="", end_iso: str="", page_limit: int=1000,
                   max_calls: int=2000, sleep_ms: int=350) -> Dict[str, object]:
        df = fetch_ohlcv_paginated(
            self.ex.exchange,
            mkt_symbol,
            timeframe=timeframe,
            start_iso=start_iso,
            end_iso=end_iso,
            limit_per_call=page_limit,
            max_calls=max_calls,
            extra_sleep_ms=sleep_ms,
        )
        if df.empty:
            raise RuntimeError("No OHLCV data for %s" % mkt_symbol)

        # Save OHLCV if DB
        if self.db:
            try:
                self.db.insert_ohlcv_rows(run_id, mkt_symbol.replace("/",""), df)
            except Exception as e:
                print("[BT][DB] insert_ohlcv_rows failed:", e)

        close = df["close"].values.astype(float)
        high  = df["high"].values.astype(float)
        low   = df["low"].values.astype(float)
        vol   = df["vol"].values.astype(float)

        atr = atr_from_ohlc(high, low, close, ATR_LEN)
        ma_s = pd.Series(close).rolling(MA_SHORT).mean().values
        ma_l = pd.Series(close).rolling(MA_LONG).mean().values

        balance = START_BALANCE_USDT
        pos_qty = 0.0
        pos_avg = 0.0
        highest = 0.0
        trades = 0
        wins = 0
        returns = []
        equity_curve = [balance]
        rows = []

        symbol_key_binance = mkt_symbol.replace("/", "")
        total = len(df)
        start_idx = max(MA_LONG, ATR_LEN, VOL_WINDOW) + 1

        last_progress = -1

        for i in range(start_idx, total):
            # ------- progress % -------
            pct = int((i - start_idx + 1) * 100.0 / max(1, (total - start_idx)))
            if pct != last_progress:
                sys.stdout.write("\r[BACKTEST %s] Progress: %d%%" % (mkt_symbol, pct))
                sys.stdout.flush()
                last_progress = pct
            # --------------------------

            last = close[i]
            if i - MOM_LOOKBACK < 0:
                if self.db:
                    try: self.db.insert_backtest_equity(run_id, symbol_key_binance, int(df["time"].iloc[i]), float(balance + pos_qty*last))
                    except Exception: pass
                equity_curve.append(balance + pos_qty * last); continue

            mom = (close[i] - close[i - MOM_LOOKBACK]) / (close[i - MOM_LOOKBACK] + 1e-12)
            vol_now = vol[i]
            vol_avg = vol[max(0, i - VOL_WINDOW):i].mean()
            atr_i = atr[i]
            ts_ms = int(df["time"].iloc[i])
            ts_iso = datetime.utcfromtimestamp(ts_ms/1000.0).isoformat() + "Z"

            if vol_avg <= 0 or (vol_now / vol_avg) < 0.7:
                if self.db:
                    try: self.db.insert_backtest_equity(run_id, symbol_key_binance, ts_ms, float(balance + pos_qty*last))
                    except Exception: pass
                equity_curve.append(balance + pos_qty * last)
                continue

            bullish = (ma_s[i] > ma_l[i]) and (mom > 0.001)
            bearish = (ma_s[i] < ma_l[i]) and (mom < -0.001)

            # manage open pos
            if pos_qty > 0:
                highest = max(highest, last)
                stop_price = max(pos_avg * (1.0 - TRAIL_PCT), highest * (1.0 - TRAIL_PCT))
                tp_price = pos_avg + 2.6 * atr_i

                if last <= stop_price or last >= tp_price:
                    sell_notional = pos_qty * last
                    slip = dynamic_slippage_pct(symbol_key_binance, sell_notional, vol_now, vol_avg, atr_i, last)
                    eff_price = last * (1.0 - slip)
                    proceeds = pos_qty * eff_price
                    fee = proceeds * TAKER_FEE_PCT
                    net = proceeds - fee
                    pnl = net - (pos_qty * pos_avg)
                    balance += net
                    trades += 1
                    if pnl > 0: wins += 1
                    denom = pos_qty * pos_avg if pos_qty * pos_avg != 0 else 1e-9
                    returns.append(pnl / denom)

                    rows.append({"datetime": ts_iso, "time_ms": ts_ms, "side": "SELL",
                                 "price": float(eff_price), "qty": float(pos_qty),
                                 "pnl_quote": float(pnl), "event": "stop_or_tp", "slippage_pct": float(slip)})
                    if self.db:
                        try: self.db.insert_backtest_trade(run_id, symbol_key_binance, ts_ms, "SELL", float(eff_price), float(pos_qty), float(pnl), "stop_or_tp", float(slip))
                        except Exception: pass

                    pos_qty = 0.0; pos_avg = 0.0; highest = 0.0
                    if self.db:
                        try: self.db.insert_backtest_equity(run_id, symbol_key_binance, ts_ms, float(balance))
                        except Exception: pass
                    equity_curve.append(balance)
                    continue

            # entry
            if pos_qty == 0 and bullish:
                stop_price = last - 1.8 * atr_i
                risk_amt = balance * BASE_RISK_PCT * ALLOCATION_TREND_PCT
                risk_per_unit = last - stop_price
                if risk_per_unit > 0:
                    qty = risk_amt / risk_per_unit
                    notional = qty * last
                    cap = balance * ALLOCATION_TREND_PCT
                    notional = min(notional, cap)
                    if notional > MIN_NOTIONAL:
                        pos_qty = round(notional / last, 6)
                        slip = dynamic_slippage_pct(symbol_key_binance, notional, vol_now, vol_avg, atr_i, last)
                        eff_price = last * (1.0 + slip)
                        cost = pos_qty * eff_price
                        fee = cost * TAKER_FEE_PCT
                        balance -= (cost + fee)
                        pos_avg = eff_price
                        highest = eff_price

                        rows.append({"datetime": ts_iso, "time_ms": ts_ms, "side": "BUY",
                                     "price": float(eff_price), "qty": float(pos_qty),
                                     "pnl_quote": 0.0, "event": "open", "slippage_pct": float(slip)})
                        if self.db:
                            try: self.db.insert_backtest_trade(run_id, symbol_key_binance, ts_ms, "BUY", float(eff_price), float(pos_qty), 0.0, "open", float(slip))
                            except Exception: pass

            # exit on flip
            if pos_qty > 0 and bearish:
                sell_notional = pos_qty * last
                slip = dynamic_slippage_pct(symbol_key_binance, sell_notional, vol_now, vol_avg, atr_i, last)
                eff_price = last * (1.0 - slip)
                proceeds = pos_qty * eff_price
                fee = proceeds * TAKER_FEE_PCT
                net = proceeds - fee
                pnl = net - (pos_qty * pos_avg)
                balance += net
                trades += 1
                if pnl > 0: wins += 1
                denom = pos_qty * pos_avg if pos_qty * pos_avg != 0 else 1e-9
                returns.append(pnl / denom)

                rows.append({"datetime": ts_iso, "time_ms": ts_ms, "side": "SELL",
                             "price": float(eff_price), "qty": float(pos_qty),
                             "pnl_quote": float(pnl), "event": "flip_exit", "slippage_pct": float(slip)})
                if self.db:
                    try: self.db.insert_backtest_trade(run_id, symbol_key_binance, ts_ms, "SELL", float(eff_price), float(pos_qty), float(pnl), "flip_exit", float(slip))
                    except Exception: pass

                pos_qty = 0.0; pos_avg = 0.0; highest = 0.0

            if self.db:
                try: self.db.insert_backtest_equity(run_id, symbol_key_binance, ts_ms, float(balance + pos_qty * last))
                except Exception: pass
            equity_curve.append(balance + pos_qty * last)

        # Finish progress line
        sys.stdout.write("\r[BACKTEST %s] Progress: 100%%\n" % mkt_symbol)
        sys.stdout.flush()

        win_rate = (wins / trades) if trades > 0 else 0.0
        mdd = max_drawdown(equity_curve)
        sr = sharpe_ratio(returns, periods_per_year=365 * 24 * 60)
        report = {
            "trades": int(trades),
            "win_rate": round(win_rate, 4),
            "max_drawdown": round(mdd, 4),
            "sharpe": round(sr, 3),
            "final_balance": round(balance + pos_qty * (close[-1] if len(close) else 0), 2),
            "start_balance": START_BALANCE_USDT,
        }

        # Export CSV
        try:
            os.makedirs(BACKTEST_EXPORT_DIR, exist_ok=True)
        except Exception:
            pass
        safe_sym = mkt_symbol.replace("/", "_")
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fname = "backtest_%s_%s_%s.csv" % (safe_sym, timeframe, stamp)
        out_path = os.path.join(BACKTEST_EXPORT_DIR or ".", fname)
        pd.DataFrame(rows).to_csv(out_path, index=False)
        report["export_csv"] = out_path

        if self.db:
            try:
                self.db.insert_backtest_summary(run_id, symbol_key_binance, int(trades), float(win_rate),
                                                float(mdd), float(sr), float(report["final_balance"]),
                                                float(report["start_balance"]), out_path)
            except Exception as e:
                print("[BT][DB] insert_backtest_summary failed:", e)

        return report

    def run_multi(self, run_id: str, markets_ccxt: List[str], timeframe: str="1m",
                  start_iso: str="", end_iso: str="",
                  page_limit: int=1000, max_calls: int=2000, sleep_ms: int=350) -> Dict[str, Dict[str, object]]:
        out = {}
        for mkt in markets_ccxt:
            try:
                rep = self.run_symbol(
                    run_id=run_id,
                    mkt_symbol=mkt,
                    timeframe=timeframe,
                    start_iso=start_iso,
                    end_iso=end_iso,
                    page_limit=page_limit,
                    max_calls=max_calls,
                    sleep_ms=sleep_ms,
                )
                out[mkt] = rep
            except Exception as e:
                out[mkt] = {"error": str(e)}
        return out


# -------------------------- Orchestrator --------------------------
class Bot(object):
    def __init__(self):
        self.account = Account()
        self.risk = RiskManager()
        self.db = MySQLClient(MYSQL_CFG) if USE_MYSQL else None
        if self.db:
            try:
                self.db.connect()
            except Exception as e:
                print("[DB] connection error:", e)
                self.db = None

        self.ws = WSManager(BINANCE_WS_URL, SYMBOLS)
        self.exchange = Exchange() if (LIVE or TESTNET or BACKTEST) else None
        self.strategy = Strategy(self.account, self.risk, self.ws, self.db, self.exchange)
        self._stop = asyncio.Event()

    async def run_live(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, lambda: asyncio.ensure_future(self.stop()))
            loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.ensure_future(self.stop()))
        except NotImplementedError:
            pass

        ws_task = asyncio.create_task(self.ws.connect())
        kl_task = asyncio.create_task(self.klines_updater())
        strat_task = asyncio.create_task(self.strategy_loop())

        done, pending = await asyncio.wait({ws_task, kl_task, strat_task}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()

    async def klines_updater(self) -> None:
        while not self._stop.is_set():
            await self.strategy.refresh_klines()
            await asyncio.sleep(45)

    async def strategy_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.strategy.maybe_log_tick()
                await self.strategy.step()
            except Exception as e:
                print("[STRAT] error:", e)
            await asyncio.sleep(STRAT_INTERVAL_SEC)

    async def stop(self) -> None:
        if self._stop.is_set():
            return
        print("[BOT] stopping...")
        self._stop.set()
        try:
            await self.ws.stop()
        except Exception:
            pass

    def run_backtest(self) -> None:
        if self.exchange is None:
            raise RuntimeError("Backtest requires ccxt installed.")
        if BACKTEST_SYMBOLS:
            markets_ccxt = [s.strip() for s in BACKTEST_SYMBOLS]
        else:
            markets_ccxt = [binance_symbol_to_ccxt(s) for s in SYMBOLS]

        run_id = datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")
        started_at = int(time.time()*1000)
        bt = Backtester(self.exchange, self.db if USE_MYSQL else None)
        results = bt.run_multi(
            run_id=run_id,
            markets_ccxt=markets_ccxt,
            timeframe=BACKTEST_TIMEFRAME,
            start_iso=BACKTEST_START,
            end_iso=BACKTEST_END,
            page_limit=BACKTEST_PAGE_LIMIT,
            max_calls=BACKTEST_MAX_CALLS,
            sleep_ms=BACKTEST_SLEEP_MS,
        )
        ended_at = int(time.time()*1000)
        if self.db:
            try:
                self.db.insert_backtest_run(run_id, started_at, ended_at, note="auto")
            except Exception as e:
                print("[BT][DB] insert_backtest_run failed:", e)

        print("\n=== BACKTEST REPORTS (per symbol) ===")
        agg_equity_end = 0.0
        agg_start = 0.0
        agg_trades = 0
        sharpe_list = []
        dd_list = []
        for mkt, rep in results.items():
            print(mkt, "=>", rep)
            if "error" in rep:
                continue
            agg_equity_end += rep.get("final_balance", 0.0)
            agg_start += rep.get("start_balance", 0.0)
            agg_trades += rep.get("trades", 0)
            sharpe_list.append(rep.get("sharpe", 0.0))
            dd_list.append(rep.get("max_drawdown", 0.0))
        if markets_ccxt:
            avg_sharpe = sum(sharpe_list)/max(1, len(sharpe_list))
            avg_dd = sum(dd_list)/max(1, len(dd_list))
            print("\n=== BACKTEST COMBINED SUMMARY ===")
            print("Symbols:", markets_ccxt)
            print("Total trades:", agg_trades)
            print("Avg Sharpe:", round(avg_sharpe, 3))
            print("Avg MaxDD:", round(avg_dd, 4))
            print("Combined start balance:", round(agg_start, 2))
            print("Combined final balance:", round(agg_equity_end, 2))
            print("CSV exports are written to:", os.path.abspath(BACKTEST_EXPORT_DIR or "."))


# -------------------------- Entry --------------------------
if __name__ == "__main__":
    print("Mode => DRY_RUN:%s | TESTNET:%s | LIVE:%s | BACKTEST:%s" % (DRY_RUN, TESTNET, LIVE, BACKTEST))
    print("Symbols:", SYMBOLS)
    if BACKTEST:
        Bot().run_backtest()
        sys.exit(0)
    try:
        asyncio.run(Bot().run_live())
    except KeyboardInterrupt:
        pass
