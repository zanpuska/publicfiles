
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
