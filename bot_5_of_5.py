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
