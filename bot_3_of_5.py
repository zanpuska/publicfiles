
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