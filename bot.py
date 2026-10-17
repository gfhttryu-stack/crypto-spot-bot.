"""Telegram crypto spot scanner v2 — seven research scanners, alert-only.

Alert-only; public market data only. No order placement, no API keys for trading.
Designed for research/testing, not investment advice.
"""
from __future__ import annotations
import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any

import ccxt.async_support as ccxt
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spotbot")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ALLOWED_CHAT_IDS = {x.strip() for x in os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "").split(",") if x.strip()}
EXCHANGES = [x.strip().lower() for x in os.getenv("EXCHANGES", "binance,bybit").split(",") if x.strip()]
TIMEFRAMES = [x.strip() for x in os.getenv("TIMEFRAMES", "30m,1h,2h,4h,12h").split(",") if x.strip()]
SCAN_SECONDS = max(60, int(os.getenv("SCAN_SECONDS", "1800")))  # retained for cooldown/config; scans are manual only
OHLCV_REFRESH_SECONDS = max(60, int(os.getenv("OHLCV_REFRESH_SECONDS", "300")))
CANDLE_LIMIT = max(60, int(os.getenv("CANDLE_LIMIT", "100")))
MIN_QUOTE_VOLUME_USDT = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "250000"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.50"))
REL_VOLUME_MIN = float(os.getenv("REL_VOLUME_MIN", "2.0"))
ALERT_SCORE_MIN = float(os.getenv("ALERT_SCORE_MIN", "45"))
MAX_SYMBOLS_PER_EXCHANGE = int(os.getenv("MAX_SYMBOLS_PER_EXCHANGE", "0"))  # 0 = all eligible USDT spot pairs
BACKTEST_MONTHS = int(os.getenv("BACKTEST_MONTHS", "12"))

TF_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
              "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800, "12h": 43200,
              "1d": 86400, "3d": 259200, "1w": 604800, "1M": 2592000}

@dataclass
class MarketHit:
    exchange: str
    symbol: str
    timeframe: str
    price: float
    change_pct: float
    atr_pct: float
    rel_volume: float
    score: float
    strategies: list[str]
    notes: list[str]
    entry: float = 0.0
    stop: float = 0.0
    targets: list[tuple[float, float, float]] | None = None  # (price, return_pct, reward/risk)
    spread_pct: float | None = None
    quote_volume_24h: float = 0.0
    setup_score: float = 0.0
    entry_score: float = 0.0
    execution_score: float = 0.0

class SpotScanner:
    def __init__(self) -> None:
        self.clients: dict[str, Any] = {}
        self.markets: dict[str, dict[str, Any]] = {}
        self.symbols: dict[str, list[str]] = {}
        self.candles: dict[tuple[str, str, str], tuple[float, pd.DataFrame]] = {}
        self.tickers_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self.last_alert: dict[tuple[str, str, str], float] = {}
        self.last_hits: list[MarketHit] = []
        self.alerts_enabled = False
        self.analysis_requested = False
        self.scan_task: asyncio.Task | None = None
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        for name in EXCHANGES:
            cls = getattr(ccxt, name, None)
            if cls is None:
                log.warning("Exchange inconnu ignoré: %s", name); continue
            opts = {"enableRateLimit": True, "timeout": 20000}
            if name == "binance": opts["options"] = {"defaultType": "spot"}
            if name == "bybit": opts["options"] = {"defaultType": "spot"}
            client = cls(opts)
            try:
                await client.load_markets()
                self.clients[name] = client
                self.markets[name] = client.markets
                syms = []
                for symbol, m in client.markets.items():
                    if not m.get("active", True) or not m.get("spot", False): continue
                    if not symbol.endswith("/USDT"): continue
                    if any(tag in symbol.upper() for tag in (":USDT",)): continue
                    syms.append(symbol)
                syms.sort()
                if MAX_SYMBOLS_PER_EXCHANGE > 0: syms = syms[:MAX_SYMBOLS_PER_EXCHANGE]
                self.symbols[name] = syms
                log.info("%s: %d paires Spot USDT chargées", name, len(syms))
            except Exception as e:
                log.exception("Impossible d'initialiser %s: %s", name, e)
                try: await client.close()
                except Exception: pass
        if not self.clients:
            raise RuntimeError("Aucun exchange disponible. Vérifie EXCHANGES et ta connexion réseau.")

    async def close(self) -> None:
        for c in self.clients.values():
            try: await c.close()
            except Exception: pass

    async def _tickers(self, name: str) -> dict[str, Any]:
        now = time.time()
        cached = self.tickers_cache.get(name)
        # Keep ticker data reasonably fresh even when the scheduled scan is infrequent.
        ticker_ttl = min(60, max(10, SCAN_SECONDS - 2))
        if cached and now - cached[0] < ticker_ttl: return cached[1]
        try:
            data = await self.clients[name].fetch_tickers()
            self.tickers_cache[name] = (now, data)
            return data
        except Exception as e:
            log.warning("fetch_tickers %s: %s", name, e)
            return cached[1] if cached else {}

    async def _ohlcv(self, name: str, symbol: str, tf: str, force: bool = False) -> pd.DataFrame | None:
        key = (name, symbol, tf); now = time.time()
        cached = self.candles.get(key)
        if cached and not force and now - cached[0] < OHLCV_REFRESH_SECONDS:
            return cached[1]
        try:
            raw = await self.clients[name].fetch_ohlcv(symbol, timeframe=tf, limit=CANDLE_LIMIT)
            if not raw or len(raw) < 35: return None
            df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
            # Exclude the currently forming candle to avoid look-ahead / unstable signals.
            if len(df) > 1: df = df.iloc[:-1].reset_index(drop=True)
            self.candles[key] = (now, df)
            return df
        except Exception as e:
            log.debug("OHLCV %s %s %s: %s", name, symbol, tf, e)
            return cached[1] if cached else None

    @staticmethod
    def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
        d = close.diff(); gain = d.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
        loss = -d.clip(upper=0).ewm(alpha=1/period, adjust=False).mean()
        rs = gain / loss.replace(0, np.nan)
        return 100 - 100 / (1 + rs)

    @staticmethod
    def _safe_ratio(a: float, b: float) -> float:
        return float(a / b) if b and np.isfinite(b) else 0.0

    def analyse(self, name: str, symbol: str, tf: str, df: pd.DataFrame, ticker: dict[str, Any],
                timeframe_frames: dict[str, pd.DataFrame] | None = None) -> MarketHit | None:
        """Seven independent-ish scanner modules. Scores are quality heuristics, not probabilities."""
        if len(df) < 60:
            return None
        o, h, l, c, v = (df[x].astype(float) for x in ("open", "high", "low", "close", "volume"))
        last = len(df) - 1
        price = float(c.iloc[last])
        if price <= 0 or not np.isfinite(price):
            return None

        # Only accept actual 24h quote volume. Do not substitute timeframe-dependent candle volume.
        qv = ticker.get("quoteVolume")
        try:
            qv = float(qv)
        except (TypeError, ValueError):
            return None
        if not np.isfinite(qv) or qv < MIN_QUOTE_VOLUME_USDT:
            return None

        bid, ask = ticker.get("bid"), ticker.get("ask")
        spread_pct = None
        try:
            bid, ask = float(bid), float(ask)
            if bid > 0 and ask >= bid:
                spread_pct = (ask - bid) / ((ask + bid) / 2) * 100
        except (TypeError, ValueError):
            pass
        # Missing spread is unknown, not zero. Reject unknown/invalid quotes conservatively.
        if spread_pct is None or not np.isfinite(spread_pct) or spread_pct > MAX_SPREAD_PCT:
            return None

        prev_close = c.shift(1)
        tr = pd.concat([(h-l), (h-prev_close).abs(), (l-prev_close).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        atr_pct_series = atr / c * 100
        atr_pct = float(atr_pct_series.iloc[-1])
        if not np.isfinite(atr_pct) or atr_pct <= 0:
            return None

        # Detect a quiet net move AND a contained path; prevents large up/down swings cancelling out.
        change3 = (price / float(c.iloc[-4]) - 1) * 100
        path3 = float((c.pct_change().abs().tail(3).sum()) * 100)
        abs_returns = c.pct_change().abs().tail(30).dropna() * 100
        move_limit = max(0.35, min(2.0, float(abs_returns.median() * 2.0))) if len(abs_returns) else 0.8
        price_stable = abs(change3) <= move_limit and path3 <= max(move_limit * 2.5, 0.9)

        # Relative volume compares a completed candle against the previous 20 completed candles.
        vol_base = float(v.iloc[-21:-1].median())
        rel_vol = self._safe_ratio(float(v.iloc[-1]), vol_base)
        atr_hist = atr_pct_series.dropna().tail(60)
        atr_percentile = float((atr_hist <= atr_pct).mean() * 100) if len(atr_hist) >= 20 else 0.0
        vol_rising = rel_vol >= REL_VOLUME_MIN
        volatility_rising = atr_percentile >= 75 and float(atr_pct_series.iloc[-1]) > float(atr_pct_series.iloc[-4])

        # Scanner 1: volatility rising while price has not travelled far.
        A = volatility_rising and price_stable
        # Scanner 2: relative volume surge while price remains contained.
        B = vol_rising and price_stable
        # Scanner 3: both conditions at once.
        C = A and B

        # Common confirmations, calculated only from closed candles.
        ema9 = c.ewm(span=9, adjust=False).mean()
        ema21 = c.ewm(span=21, adjust=False).mean()
        ema50 = c.ewm(span=50, adjust=False).mean()
        ema_cross = ema9.iloc[-1] > ema21.iloc[-1] and ema9.iloc[-2] <= ema21.iloc[-2]
        trend_up = price > ema21.iloc[-1] and ema21.iloc[-1] >= ema21.iloc[-4] and ema9.iloc[-1] >= ema21.iloc[-1]

        # OBV: use direction-weighted volume; require slope and stable/constructive price, not a one-bar jump.
        obv = (np.sign(c.diff()).fillna(0) * v).cumsum()
        obv_up = len(obv) >= 12 and obv.iloc[-1] > obv.iloc[-6] and obv.iloc[-6] >= obv.iloc[-11]
        price_not_breaking_down = price >= float(c.iloc[-6]) * (1 - 0.005)
        scanner_obv = obv_up and price_not_breaking_down

        # Scanner 5: fresh bullish EMA cross, with trend context to reduce chop.
        scanner_ema = bool(ema_cross and ema21.iloc[-1] >= ema21.iloc[-4])

        # Scanner 6: bullish candle patterns, only on closed candle and near lower/sideways structure.
        rng = float(h.iloc[-1] - l.iloc[-1])
        body = abs(float(c.iloc[-1] - o.iloc[-1]))
        lower_wick = min(float(o.iloc[-1]), float(c.iloc[-1])) - float(l.iloc[-1])
        bullish_engulf = (c.iloc[-1] > o.iloc[-1] and c.iloc[-2] < o.iloc[-2]
                          and o.iloc[-1] <= c.iloc[-2] and c.iloc[-1] >= o.iloc[-2])
        hammer = rng > 0 and lower_wick >= body * 2 and (float(h.iloc[-1]) - max(float(o.iloc[-1]), float(c.iloc[-1]))) <= max(body, rng * .25) and c.iloc[-1] > o.iloc[-1]
        prior_down_or_flat = float(c.iloc[-5]) >= float(c.iloc[-1]) or abs(change3) <= move_limit
        scanner_candle = bool((bullish_engulf or hammer) and prior_down_or_flat)

        # Scanner 7: MACD cross and improving histogram; a standalone cross isn't enough.
        macd_line = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        hist = macd_line - macd_signal
        scanner_macd = bool(macd_line.iloc[-1] > macd_signal.iloc[-1] and macd_line.iloc[-2] <= macd_signal.iloc[-2]
                            and hist.iloc[-1] > hist.iloc[-2])

        # Higher timeframe context: use the nearest available higher TF; never accept an arbitrary stale cache.
        tf_order = ["30m", "1h", "2h", "4h", "12h"]
        frames = timeframe_frames or {}
        higher = [x for x in tf_order if x in TIMEFRAMES and tf in tf_order and tf_order.index(x) > tf_order.index(tf)]
        htf_ok = None
        if higher:
            hdf = frames.get(higher[0])
            if hdf is not None and len(hdf) >= 25:
                hc = hdf.close.astype(float)
                hema = hc.ewm(span=20, adjust=False).mean()
                htf_ok = bool(hc.iloc[-1] > hema.iloc[-1] and hema.iloc[-1] >= hema.iloc[-3])

        # A custom bullish rating; this is NOT TradingView's proprietary rating.
        rsi = self._rsi(c)
        rsi_now = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else 50.0
        rsi_ok = 48 <= rsi_now <= 72 and rsi_now >= float(rsi.iloc[-3] if pd.notna(rsi.iloc[-3]) else rsi_now)
        macd_bull = macd_line.iloc[-1] > macd_signal.iloc[-1] and hist.iloc[-1] >= hist.iloc[-2]
        bullish_votes = sum([bool(trend_up), bool(ema9.iloc[-1] > ema21.iloc[-1]), bool(macd_bull), bool(rsi_ok), bool(obv_up)])
        rating = "STRONG BUY" if bullish_votes >= 4 and (htf_ok is not False) else "BUY" if bullish_votes >= 3 and (htf_ok is not False) else "NEUTRAL"
        # User explicitly asked to show only Buy/Strong Buy opportunities.
        if rating not in ("BUY", "STRONG BUY"):
            return None

        strategies: list[str] = []
        notes: list[str] = []
        core_trigger = False
        if A:
            strategies.append("Scanner 1: volatilité en hausse + prix contenu")
            notes.append(f"ATR {atr_pct:.2f}% (percentile {atr_percentile:.0f})")
            core_trigger = True
        if B:
            strategies.append("Scanner 2: volume relatif en hausse + prix contenu")
            notes.append(f"Volume relatif {rel_vol:.2f}x la médiane des 20 bougies précédentes")
            core_trigger = True
        if C:
            strategies.append("Scanner 3: confluence volatilité + volume")
            core_trigger = True
        if scanner_obv:
            strategies.append("Scanner 4: OBV en progression pendant que le prix reste contenu")
        if scanner_ema:
            strategies.append("Scanner 5: croisement haussier EMA9/EMA21 confirmé")
        if scanner_candle:
            strategies.append("Scanner 6: formation de bougie haussière confirmée")
        if scanner_macd:
            strategies.append("Scanner 7: croisement MACD haussier avec histogramme en amélioration")

        # A scanner-specific trigger is required; generic green indicators alone do not create alerts.
        if not (core_trigger or scanner_obv or scanner_ema or scanner_candle or scanner_macd):
            return None
        if htf_ok is True:
            notes.append("Timeframe supérieur haussier (EMA20 montante)")
        elif htf_ok is False:
            notes.append("Timeframe supérieur contradictoire")
        if trend_up:
            notes.append("Tendance EMA favorable")
        notes.append(f"Notation technique interne: {rating} ({bullish_votes}/5 confirmations)")
        notes.append(f"Mouvement net 3 bougies {change3:+.2f}%; amplitude cumulée {path3:.2f}%")

        # Three transparent scores. These are heuristic quality scores, never win probabilities.
        signal_score = 25 * int(A) + 25 * int(B) + 15 * int(scanner_obv) + 10 * int(scanner_ema) + 10 * int(scanner_candle) + 10 * int(scanner_macd) + 5 * int(htf_ok is True)
        signal_score = min(100.0, float(signal_score))
        entry_score = max(0.0, min(100.0, 100 - max(0.0, abs(change3) - move_limit) * 12 - max(0.0, path3 - move_limit * 2) * 8))
        execution_score = max(0.0, min(100.0, 100 - spread_pct / MAX_SPREAD_PCT * 40 + min(20.0, qv / max(MIN_QUOTE_VOLUME_USDT, 1) * 2)))

        # Technical stop: below recent swing low and ATR buffer. Do not force trade if target geometry is poor.
        swing_low = float(l.iloc[-12:-1].min())
        stop = min(swing_low - 0.25 * float(atr.iloc[-1]), price - 1.2 * float(atr.iloc[-1]))
        if stop <= 0 or stop >= price:
            return None
        risk_pct = (price - stop) / price * 100
        # Targets are resistance-based when plausible, otherwise risk multiples; require >=2R to a realistic target.
        resistances = sorted(set(float(x) for x in h.iloc[-40:-1].nlargest(8).tolist() if float(x) > price * 1.002))
        nearest_barrier = resistances[0] if resistances else None
        min_target = price + 2.0 * (price - stop)
        # Do not pretend a distant resistance is reachable if a nearer barrier blocks the 2R target.
        if nearest_barrier is not None and nearest_barrier < min_target:
            return None
        candidates = [(r, (r / price - 1) * 100, (r-price)/(price-stop)) for r in resistances if r >= min_target]
        if not candidates:
            candidates = [(min_target, (min_target / price - 1) * 100, 2.0)]
        # Distinct ascending targets; farther targets are only added if no nearer resistance blocks them.
        targets = candidates[:3]
        if len(targets) < 2:
            r2 = price + 2.5 * (price - stop)
            if not resistances or min(resistances) > r2:
                targets.append((r2, (r2 / price - 1) * 100, 2.5))
        if len(targets) < 3:
            r3 = price + 3.0 * (price - stop)
            if not resistances or min(resistances) > r3:
                targets.append((r3, (r3 / price - 1) * 100, 3.0))
        if not targets:
            return None

        return MarketHit(name, symbol, tf, price, change3, atr_pct, rel_vol, signal_score,
                         strategies, notes, price, stop, targets, spread_pct, qv,
                         signal_score, entry_score, execution_score)

    async def scan_once(self, force: bool = False) -> list[MarketHit]:
        async with self.lock:
            hits: list[MarketHit] = []
            # Rotate symbols to distribute API requests. Each scan checks a slice; OHLCV cache reduces repeat calls.
            for name, symbols in self.symbols.items():
                tickers = await self._tickers(name)
                if not symbols: continue
                # Rotate per scan so the whole market is covered without flooding rate limits.
                batch_size = max(20, min(120, int(os.getenv("SYMBOLS_PER_SCAN", "60"))))
                start_idx = int(time.time() // SCAN_SECONDS * batch_size) % len(symbols)
                batch = [symbols[(start_idx+i) % len(symbols)] for i in range(min(batch_size, len(symbols)))]
                for symbol in batch:
                    ticker = tickers.get(symbol, {})
                    if not ticker: continue
                    frames: dict[str, pd.DataFrame] = {}
                    for tf in TIMEFRAMES:
                        df = await self._ohlcv(name, symbol, tf, force=force)
                        if df is not None:
                            frames[tf] = df
                    # Analyze only after all selected timeframes are fetched, so strategy 13
                    # can use same-symbol higher-timeframe data instead of stale cache entries.
                    for tf, df in frames.items():
                        hit = self.analyse(name, symbol, tf, df, ticker, frames)
                        if hit:
                            # Never hide the user's core A/B/C detections behind the
                            # general score threshold; complementary strategies still use it.
                            core_hit = any(item.startswith(("Scanner 1:", "Scanner 2:", "Scanner 3:")) for item in hit.strategies)
                            if core_hit or hit.score >= ALERT_SCORE_MIN:
                                hits.append(hit)
            hits.sort(key=lambda x: (x.score, x.rel_volume, x.atr_pct), reverse=True)
            self.last_hits = hits[:100]
            return self.last_hits

    async def backtest(self, name: str, symbol: str, tf: str, months: int = BACKTEST_MONTHS) -> str:
        if name not in self.clients: return f"Exchange {name} non initialisé."
        if symbol not in self.markets[name]: return f"Paire {symbol} introuvable sur {name}. Exemple attendu : BTC/USDT"
        since = int((time.time() - months*30.4375*86400) * 1000)
        rows: list[list[float]] = []; cursor = since
        try:
            while cursor < int(time.time()*1000):
                batch = await self.clients[name].fetch_ohlcv(symbol, timeframe=tf, since=cursor, limit=1000)
                if not batch: break
                rows.extend(batch)
                next_cursor = int(batch[-1][0]) + 1
                if next_cursor <= cursor: break
                cursor = next_cursor
                if len(batch) < 2: break
                await asyncio.sleep(self.clients[name].rateLimit / 1000)
            df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"]).drop_duplicates("timestamp").sort_values("timestamp")
            if len(df) < 100: return f"Données insuffisantes pour {symbol} {tf} sur {months} mois ({len(df)} bougies)."
            df = df.astype({k: float for k in ["open","high","low","close","volume"]}).reset_index(drop=True)
            # Basic, deliberately transparent A/B/C proxy backtest: signal at candle close, entry next candle open,
            # exit after 6 candles. This is a screening test, not a full execution simulator.
            trades=[]; warm=40
            for i in range(warm, len(df)-7):
                sub=df.iloc[:i+1]
                c=sub.close; h=sub.high; l=sub.low; v=sub.volume; price=float(c.iloc[-1])
                prev=c.shift(1); tr=pd.concat([(h-l),(h-prev).abs(),(l-prev).abs()],axis=1).max(axis=1)
                atr=tr.rolling(14).mean(); atr_pct=(atr/ c*100).dropna()
                atrp=float((atr_pct.tail(60)<=float(atr.iloc[-1]/price*100)).mean()) if len(atr_pct) else 0
                move=float(abs(price/float(c.iloc[-4])-1)*100)
                chg=(c/c.shift(1)-1).abs().tail(20).dropna()
                lim=max(.6,min(3.0,float(chg.median()*100*2.5))) if len(chg) else 1.5
                rel=self._safe_ratio(float(v.iloc[-1]),float(v.iloc[-21:-1].median()))
                A=atrp>=.80 and move<=lim
                B=rel>=REL_VOLUME_MIN and move<=lim
                if A and B:
                    entry=float(df.open.iloc[i+1]); exitp=float(df.close.iloc[i+6])
                    if entry>0: trades.append((exitp/entry-1)*100)
            if not trades: return f"Backtest A+B ({name} {symbol} {tf}, {months} mois) : aucun signal détecté. Ce n'est pas une preuve d'absence d'opportunités en temps réel."
            arr=np.array(trades); win=float((arr>0).mean()*100); avg=float(arr.mean())
            return (f"Backtest exploratoire A+B — {name} {symbol} {tf}, {months} mois\n"
                    f"Signaux simulés : {len(arr)}\nTaux de trades positifs : {win:.1f}%\n"
                    f"Rendement moyen brut par trade : {avg:+.2f}%\nMeilleur : {arr.max():+.2f}% | Pire : {arr.min():+.2f}%\n\n"
                    "Limites : résultats bruts, frais/spread/slippage non inclus; sortie fixe après 6 bougies; sélection survivante; pas de validation hors échantillon. Ne pas interpréter comme une performance attendue.")
        except Exception as e:
            log.exception("Backtest error")
            return f"Erreur backtest : {e}"

scanner = SpotScanner()

async def authorized(update: Update) -> bool:
    chat = update.effective_chat
    if chat is None: return False
    if ALLOWED_CHAT_IDS and str(chat.id) not in ALLOWED_CHAT_IDS:
        if update.effective_message: await update.effective_message.reply_text("Accès non autorisé.")
        return False
    return True

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await authorized(update): return
    await update.effective_message.reply_text(
        "Bot crypto Spot — analyses manuelles uniquement. Aucun ordre n'est passé.\n\n"
        "Envoie 1 à 7 pour lancer uniquement la stratégie correspondante.\n"
        "Envoie BTC, DOT, CRV ou BTC/USDT pour une analyse complète de la paire.\n"
        "/scan — scan général (lot de paires)\n/top — résultats du dernier scan\n"
        "/status — état du bot\n/help — aide\n"
        "/backtest EXCHANGE SYMBOL TIMEFRAME — ex. /backtest binance ETH/USDT 1h")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await authorized(update): return
    await start_cmd(update, context)

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await authorized(update): return
    msg = (f"Échanges : {', '.join(scanner.clients) or 'initialisation'}\n"
           f"Paires chargées : {sum(map(len, scanner.symbols.values()))}\nTimeframes : {', '.join(TIMEFRAMES)}\n"
           f"Mode scan : manuel uniquement ; refresh OHLCV : {OHLCV_REFRESH_SECONDS}s\n"
           f"Alertes automatiques : {'ON' if scanner.alerts_enabled else 'OFF'}\n"
           f"Seuil volume relatif : {REL_VOLUME_MIN}x ; score minimum : {ALERT_SCORE_MIN}\n"
           "Mode trading : aucun ordre, données publiques uniquement.")
    await update.effective_message.reply_text(msg)

async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await authorized(update): return
    await update.effective_message.reply_text("Analyse lancée. La couverture est répartie en lots pour respecter les limites des exchanges…")
    hits = await scanner.scan_once(force=True)
    await send_hits(context.application, update.effective_chat.id, hits, max_items=10)

async def numbered_strategy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, number: int) -> None:
    if not await authorized(update): return
    labels = {
        1: "volatilité en hausse + prix encore contenu",
        2: "volume relatif en hausse + prix encore contenu",
        3: "confluence volatilité + volume",
        4: "OBV en progression pendant que le prix reste contenu",
        5: "croisement haussier EMA9/EMA21 confirmé",
        6: "formation de bougie haussière confirmée",
        7: "croisement MACD haussier avec histogramme en amélioration",
    }
    await update.effective_message.reply_text(f"Scan manuel de la stratégie {number} : {labels[number]}. Je lance l'analyse…")
    hits = await scanner.scan_once(force=True)
    needle = f"Scanner {number}:"
    selected = [h for h in hits if any(x.startswith(needle) for x in h.strategies)]
    selected.sort(key=lambda x: (x.score, x.rel_volume, x.atr_pct), reverse=True)
    if not selected:
        await update.effective_message.reply_text(f"Aucun signal BUY/STRONG BUY pour la stratégie {number} dans le lot analysé. Cela ne signifie pas qu'aucun signal n'existe sur toutes les paires.")
        return
    await send_hits(context.application, update.effective_chat.id, selected, max_items=10)

async def pair_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_symbol: str) -> None:
    if not await authorized(update): return
    raw = raw_symbol.strip().upper().replace(" ", "")
    if "/" not in raw:
        base = raw
        symbol = f"{base}/USDT"
    else:
        parts = raw.split("/")
        if len(parts) != 2 or not all(parts):
            await update.effective_message.reply_text("Format attendu : BTC, DOT, CRV ou BTC/USDT.")
            return
        symbol = f"{parts[0]}/{parts[1]}"
    await update.effective_message.reply_text(f"Analyse complète de {symbol} sur les marchés Spot disponibles…")
    reports = []
    for name in list(scanner.clients):
        if symbol not in scanner.markets.get(name, {}):
            continue
        ticker = (await scanner._tickers(name)).get(symbol, {})
        if not ticker:
            continue
        sections = [f"📊 {name.upper()} SPOT — {symbol}"]
        last = None
        for tf in TIMEFRAMES:
            df = await scanner._ohlcv(name, symbol, tf, force=True)
            if df is None or len(df) < 30:
                sections.append(f"{tf}: données insuffisantes")
                continue
            c = df.close.astype(float); h = df.high.astype(float); l = df.low.astype(float); v = df.volume.astype(float)
            price = float(c.iloc[-1]); last = price
            ema9 = c.ewm(span=9, adjust=False).mean(); ema21 = c.ewm(span=21, adjust=False).mean(); ema50 = c.ewm(span=50, adjust=False).mean()
            delta = c.diff(); gain = delta.clip(lower=0).rolling(14).mean(); loss = -delta.clip(upper=0).rolling(14).mean()
            rs = gain / loss.replace(0, np.nan); rsi = 100 - 100 / (1 + rs); rsi_now = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else 50.0
            fast = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean(); sig = fast.ewm(span=9, adjust=False).mean(); hist = fast - sig
            prev = c.shift(1); tr = pd.concat([(h-l), (h-prev).abs(), (l-prev).abs()], axis=1).max(axis=1); atr = float(tr.rolling(14).mean().iloc[-1])
            obv = (np.sign(c.diff()).fillna(0) * v).cumsum(); obv_up = float(obv.iloc[-1]) > float(obv.iloc[-6])
            trend = "haussière" if price > float(ema21.iloc[-1]) and ema21.iloc[-1] >= ema21.iloc[-3] else "baissière" if price < float(ema21.iloc[-1]) and ema21.iloc[-1] <= ema21.iloc[-3] else "latérale / mixte"
            macd_state = "haussier" if fast.iloc[-1] > sig.iloc[-1] and hist.iloc[-1] >= hist.iloc[-2] else "baissier ou en affaiblissement"
            ema_state = "EMA9 > EMA21 > EMA50" if ema9.iloc[-1] > ema21.iloc[-1] > ema50.iloc[-1] else "alignement EMA non entièrement haussier"
            support = float(l.iloc[-30:].min()); resistance = float(h.iloc[-30:].max())
            rel_vol = float(v.iloc[-1] / max(float(v.iloc[-21:-1].median()), 1e-12))
            change = (price / float(c.iloc[-2]) - 1) * 100
            sections.append(f"{tf} — prix {price:.8g} | variation bougie clôturée {change:+.2f}%\nTendance: {trend}; {ema_state}\nRSI(14): {rsi_now:.1f}; MACD: {macd_state}; OBV: {'en hausse' if obv_up else 'non confirmé'}\nATR(14): {atr:.8g} ({atr/price*100:.2f}%) | volume relatif: {rel_vol:.2f}x\nSupport observé 30 bougies: {support:.8g} | résistance observée: {resistance:.8g}")
        if last is not None:
            qv = ticker.get("quoteVolume")
            sections.append(f"Volume 24 h estimé: {float(qv):,.0f} {symbol.split('/')[-1]}" if qv else "Volume 24 h: non fourni par la plateforme")
        reports.append("\n\n".join(sections))
    if not reports:
        await update.effective_message.reply_text(f"Paire {symbol} introuvable sur Binance/Bybit Spot. Essaie une paire cotée en USDT, par exemple BTC/USDT.")
        return
    result = "\n\n━━━━━━━━━━━━\n\n".join(reports)
    result += "\n\n⚠️ Analyse technique descriptive, pas une garantie de hausse. Supports/résistances calculés sur les 30 dernières bougies de chaque unité de temps. Aucun ordre n'est passé."
    # Telegram messages have a size limit; split safely into chunks.
    for i in range(0, len(result), 3800):
        await update.effective_message.reply_text(result[i:i+3800])

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await authorized(update): return
    msg = (update.effective_message.text or "").strip()
    if msg in {str(i) for i in range(1, 8)}:
        await numbered_strategy_cmd(update, context, int(msg))
        return
    if re.fullmatch(r"[A-Za-z0-9]{2,15}(?:/[A-Za-z0-9]{2,15})?", msg):
        await pair_analysis(update, context, msg)

async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await authorized(update): return
    await send_hits(context.application, update.effective_chat.id, scanner.last_hits, max_items=10)

async def alerts_on_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await authorized(update): return
    scanner.alerts_enabled = False; await update.effective_message.reply_text("Les alertes automatiques sont désactivées définitivement dans ce mode. Utilise /scan, 1–7 ou le symbole d'une paire pour lancer une analyse manuelle.")

async def alerts_off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await authorized(update): return
    scanner.alerts_enabled = False; await update.effective_message.reply_text("Mode manuel uniquement : aucun scan ni aucune alerte automatique. /scan, 1–7 et les symboles restent disponibles.")

async def strategy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await authorized(update): return
    name = update.message.text.split()[0].lower()
    text = {
      "/strategiea": "A — volatilité élevée (ATR% dans le haut de sa propre distribution récente) ET mouvement récent inférieur à un seuil adaptatif.",
      "/strategieb": f"B — volume de la dernière bougie clôturée >= {REL_VOLUME_MIN}x la médiane des 20 bougies précédentes ET mouvement récent limité.",
      "/strategiec": "C — A et B doivent être vraies simultanément. C est le filtre central; il ne prédit pas le sens du mouvement."
    }.get(name, "Stratégie inconnue.")
    await update.effective_message.reply_text(text)

async def backtest_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await authorized(update): return
    if len(context.args) < 3:
        await update.effective_message.reply_text("Usage : /backtest binance ETH/USDT 1h"); return
    name, symbol, tf = context.args[0].lower(), context.args[1].upper(), context.args[2]
    if tf not in TF_SECONDS:
        await update.effective_message.reply_text(f"Timeframe inconnu. Exemples : {', '.join(TIMEFRAMES)}"); return
    await update.effective_message.reply_text(f"Backtest exploratoire {name} {symbol} {tf} sur {BACKTEST_MONTHS} mois…")
    result = await scanner.backtest(name, symbol, tf)
    await update.effective_message.reply_text(result)

async def send_hits(app: Application, chat_id: int, hits: list[MarketHit], max_items: int = 10) -> None:
    if not hits:
        await app.bot.send_message(chat_id=chat_id, text="Aucun signal ne passe actuellement les filtres dans l'échantillon analysé. Le scan couvre les paires par lots; réessaie plus tard.")
        return
    for h in hits[:max_items]:
        targets_text = "\n".join(f"TP{i}: {px:.8g} USDT ({pct:+.2f}%, R/R {rr:.2f}:1)" for i, (px, pct, rr) in enumerate((h.targets or [])[:3], 1)) or "Objectifs non disponibles"
        stop_pct = (h.stop / h.entry - 1) * 100 if h.entry else 0.0
        msg = (f"🟢 {h.exchange.upper()} SPOT — {h.symbol} ({h.timeframe})\n"
               f"Notation interne: BUY / STRONG BUY\n"
               f"Prix d'entrée indicatif: {h.entry:.8g} USDT\n"
               f"Volume relatif: {h.rel_volume:.2f}x | Volume 24h: {h.quote_volume_24h:,.0f} USDT\n"
               f"ATR: {h.atr_pct:.2f}% | Spread: {h.spread_pct:.3f}%\n"
               f"Score signal: {h.setup_score:.0f}/100 (pas une probabilité)\n"
               f"Score entrée: {h.entry_score:.0f}/100 | Exécution: {h.execution_score:.0f}/100\n"
               f"Stop-loss calculé: {h.stop:.8g} USDT ({stop_pct:+.2f}%)\n{targets_text}\n\n"
               + "\n".join(f"• {s}" for s in h.strategies[:7])
               + ("\n\nJustification/risques: " + "; ".join(h.notes) if h.notes else "")
               + "\n\nHorizon: indicatif, non prédictif. Pourcentage de réussite réel non disponible sans validation historique. Alerte d'analyse uniquement — aucun ordre n'est passé.")
        await app.bot.send_message(chat_id=chat_id, text=msg)

async def scanner_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        hits = await scanner.scan_once()
        if not scanner.alerts_enabled or not hits: return
        now = time.time()
        for h in hits[:10]:
            key = (h.exchange, h.symbol, h.timeframe)
            if now - scanner.last_alert.get(key, 0) < 1800: continue  # 30-min cooldown per pair/timeframe
            for chat_id in ALLOWED_CHAT_IDS:
                await send_hits(context.application, int(chat_id), [h], max_items=1)
            scanner.last_alert[key] = now
    except Exception:
        log.exception("Erreur du scan automatique")

async def post_init(app: Application) -> None:
    await scanner.start()
    # Intentionally no scheduled job: user-requested manual scans only.
    log.info("Bot démarré")

async def post_shutdown(app: Application) -> None:
    await scanner.close()

async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN absent. Copie .env.example vers .env et ajoute le token BotFather.")
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    app.add_handler(CommandHandler("start", start_cmd)); app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status_cmd)); app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("top", top_cmd)); app.add_handler(CommandHandler("alerts_on", alerts_on_cmd))
    app.add_handler(CommandHandler("alerts_off", alerts_off_cmd)); app.add_handler(CommandHandler("strategieA", strategy_cmd))
    app.add_handler(CommandHandler("strategieB", strategy_cmd)); app.add_handler(CommandHandler("strategieC", strategy_cmd))
    app.add_handler(CommandHandler("backtest", backtest_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    await app.initialize(); await app.start(); await app.updater.start_polling()
    try:
        while True: await asyncio.sleep(3600)
    finally:
        await app.updater.stop(); await app.stop(); await app.shutdown()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
