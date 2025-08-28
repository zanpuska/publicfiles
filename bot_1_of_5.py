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