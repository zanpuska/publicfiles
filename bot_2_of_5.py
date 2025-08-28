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
