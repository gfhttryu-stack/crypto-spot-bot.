
import asyncio
import logging
import os

import ccxt.async_support as ccxt
import pandas as pd
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ==================== CONFIGURATION ====================

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
EXCHANGE_NAMES = ["binance", "bybit"]
TIMEFRAMES = ["30m", "1h", "2h", "4h", "12h"]
MAX_PAIRS = 20
CANDLE_LIMIT = 120
BACKTEST_LIMIT = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("Dexter")

scanner = None
last_scan = 0.0


# ==================== SCANNER ====================

class SpotScanner:
    def __init__(self):
        self.clients = {}
        self.markets = {}

    async def start(self):
        for name in EXCHANGE_NAMES:
            client = None
            try:
                client_class = getattr(ccxt, name)
                client = client_class({
                    "enableRateLimit": True,
                    "timeout": 20000,
                    "options": {"defaultType": "spot"},
                })

                markets = await client.load_markets()
                self.clients[name] = client
                self.markets[name] = markets

                count = len(self.symbols(name))
                log.info("%s initialisé : %s paires Spot USDT", name, count)

            except Exception:
                log.exception("Échec initialisation %s", name)
                if client is not None:
                    await client.close()

    async def close(self):
        for client in list(self.clients.values()):
            try:
                await client.close()
            except Exception:
                log.exception("Erreur fermeture exchange")
        self.clients.clear()
        self.markets.clear()

    def symbols(self, name):
        markets = self.markets.get(name, {})
        return [
            symbol
            for symbol, market in markets.items()
            if symbol.endswith("/USDT")
            and market.get("spot")
            and market.get("active") is not False
            and ":" not in symbol
        ]

    async def candles(self, exchange, symbol, timeframe, limit):
        client = self.clients.get(exchange)
        if client is None:
            raise RuntimeError(f"Exchange {exchange} non initialisé")

        if symbol not in self.markets.get(exchange, {}):
            raise ValueError(f"Paire {symbol} introuvable sur {exchange}")

        data = await client.fetch_ohlcv(
            symbol, timeframe=timeframe, limit=limit
        )

        if not data:
            raise RuntimeError("Aucune bougie reçue")

        frame = pd.DataFrame(
            data,
            columns=[
                "timestamp", "open", "high",
                "low", "close", "volume"
            ],
        )
        frame["timestamp"] = pd.to_datetime(
            frame["timestamp"], unit="ms", utc=True
        )
        return frame

    @staticmethod
    def analyze(frame):
        if len(frame) < 55:
            return {"signal": "WAIT", "reason": "Historique insuffisant"}

        close = frame["close"]
        volume = frame["volume"]

        ema20 = close.ewm(span=20, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()

        delta = close.diff()
        gains = delta.clip(lower=0).ewm(
            alpha=1 / 14, adjust=False
        ).mean()
        losses = (-delta.clip(upper=0)).ewm(
            alpha=1 / 14, adjust=False
        ).mean()

        rs = gains / losses.replace(0, float("nan"))
        rsi = 100 - (100 / (1 + rs))
        avg_volume = volume.rolling(20).mean()

        i = len(frame) - 1
        previous = i - 1

        trend_up = ema20.iloc[i] > ema50.iloc[i]
        fresh_cross = (
            ema20.iloc[previous] <= ema50.iloc[previous]
            and trend_up
        )

        rsi_value = (
            float(rsi.iloc[i])
            if pd.notna(rsi.iloc[i]) else 50.0
        )

        volume_ratio = (
            float(volume.iloc[i] / avg_volume.iloc[i])
            if pd.notna(avg_volume.iloc[i])
            and avg_volume.iloc[i] > 0
            else 1.0
        )

        price = float(close.iloc[i])

        if (
            trend_up
            and 50 <= rsi_value <= 68
            and (fresh_cross or volume_ratio >= 1.25)
        ):
            return {
                "signal": "BUY_SETUP",
                "price": price,
                "rsi": rsi_value,
                "volume_ratio": volume_ratio,
                "reason": "Tendance haussière et momentum confirmé",
            }

        return {
            "signal": "WAIT",
            "price": price,
            "rsi": rsi_value,
            "volume_ratio": volume_ratio,
            "reason": "Filtres non validés",
        }

    async def scan(self, timeframe):
        results = []
        errors = []

        for name in EXCHANGE_NAMES:
            client = self.clients.get(name)

            if client is None:
                errors.append(f"{name}: non initialisé")
                continue

            symbols = self.symbols(name)

            try:
                tickers = await client.fetch_tickers(symbols)
                symbols.sort(
                    key=lambda s: float(
                        (tickers.get(s) or {}).get("quoteVolume") or 0
                    ),
                    reverse=True,
                )
            except Exception as exc:
                log.warning("Volume indisponible %s: %s", name, exc)

            for symbol in symbols[:MAX_PAIRS]:
                try:
                    frame = await self.candles(
                        name, symbol, timeframe, CANDLE_LIMIT
                    )
                    result = self.analyze(frame)

                    if result["signal"] == "BUY_SETUP":
                        results.append((name, symbol, result))

                except Exception as exc:
                    log.debug("%s %s: %s", name, symbol, exc)

                await asyncio.sleep(0.15)

        return results, errors

    async def backtest(self, exchange, symbol, timeframe):
        frame = await self.candles(
            exchange, symbol, timeframe, BACKTEST_LIMIT
        )

        if len(frame) < 100:
            raise RuntimeError("Historique insuffisant pour le backtest")

        close = frame["close"]
        volume = frame["volume"]

        ema20 = close.ewm(span=20, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()

        delta = close.diff()
        gains = delta.clip(lower=0).ewm(
            alpha=1 / 14, adjust=False
        ).mean()
        losses = (-delta.clip(upper=0)).ewm(
            alpha=1 / 14, adjust=False
        ).mean()

        rs = gains / losses.replace(0, float("nan"))
        rsi = 100 - (100 / (1 + rs))
        avg_volume = volume.rolling(20).mean()
        volume_ratio = volume / avg_volume

        fee = 0.001
        position = None
        trades = []

        for i in range(51, len(frame) - 1):
            trend = ema20.iloc[i] > ema50.iloc[i]
            previous_trend = ema20.iloc[i - 1] > ema50.iloc[i - 1]
            rsi_value = rsi.iloc[i]
            vol_value = volume_ratio.iloc[i]

            entry = (
                trend
                and pd.notna(rsi_value)
                and 50 <= rsi_value <= 68
                and (
                    not previous_trend
                    or (
                        pd.notna(vol_value)
                        and vol_value >= 1.25
                    )
                )
            )

            exit_signal = (
                ema20.iloc[i] < ema50.iloc[i]
                or (
                    pd.notna(rsi_value)
                    and rsi_value >= 72
                )
            )

            next_price = float(close.iloc[i + 1])

            if position is None and entry:
                position = next_price

            elif position is not None and exit_signal:
                pnl = (
                    next_price * (1 - fee)
                    / (position * (1 + fee)) - 1
                ) * 100
                trades.append(pnl)
                position = None

        if position is not None:
            final_price = float(close.iloc[-1])
            pnl = (
                final_price * (1 - fee)
                / (position * (1 + fee)) - 1
            ) * 100
            trades.append(pnl)

        equity = 1.0
        peak = 1.0
        max_drawdown = 0.0

        for pnl in trades:
            equity *= 1 + pnl / 100
            peak = max(peak, equity)
            max_drawdown = min(
                max_drawdown, equity / peak - 1
            )

        return {
            "candles": len(frame),
            "trades": len(trades),
            "win_rate": (
                sum(p > 0 for p in trades) / len(trades) * 100
                if trades else 0
            ),
            "return": (equity - 1) * 100,
            "average": (
                sum(trades) / len(trades) if trades else 0
            ),
            "drawdown": max_drawdown * 100,
        }


# ==================== TELEGRAM ====================

async def post_init(app):
    global scanner
    scanner = SpotScanner()
    await scanner.start()
    log.info(
        "Dexter prêt. Exchanges actifs : %s",
        ", ".join(scanner.clients) or "aucun",
    )


async def post_shutdown(app):
    global scanner
    if scanner is not None:
        await scanner.close()
        scanner = None


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "🤖 DEXTER — Scanner crypto Spot\n\n"
        "/status — état des exchanges\n"
        "/scan — lancer le scanner\n"
        "/tf 30m — choisir l'unité de temps\n"
        "/backtest binance BTC/USDT 1h — tester une paire\n"
        "/help — aide\n\n"
        "Timeframes : 30m, 1h, 2h, 4h, 12h\n"
        "Alertes uniquement : aucun ordre automatique."
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if scanner is None:
        await update.effective_message.reply_text(
            "Scanner non initialisé. Consulte les logs FadeHost."
        )
        return

    lines = ["📊 État de Dexter"]

    for name in EXCHANGE_NAMES:
        if name in scanner.clients:
            lines.append(
                f"✅ {name}: connecté — "
                f"{len(scanner.symbols(name))} paires Spot USDT"
            )
        else:
            lines.append(f"❌ {name}: non initialisé")

    lines.append(
        "Timeframe : "
        + context.application.bot_data.get("timeframe", "1h")
    )

    await update.effective_message.reply_text("\n".join(lines))


async def tf_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0] not in TIMEFRAMES:
        await update.effective_message.reply_text(
            "Utilisation : /tf 30m, /tf 1h, /tf 2h, /tf 4h ou /tf 12h"
        )
        return

    context.application.bot_data["timeframe"] = context.args[0]
    await update.effective_message.reply_text(
        f"✅ Timeframe réglé sur {context.args[0]}"
    )


async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global last_scan

    if scanner is None:
        await update.effective_message.reply_text(
            "Scanner non initialisé. Consulte les logs FadeHost."
        )
        return

    now = asyncio.get_running_loop().time()
    if now - last_scan < 30:
        await update.effective_message.reply_text(
            "Attends 30 secondes entre deux scans."
        )
        return

    last_scan = now
    timeframe = context.application.bot_data.get("timeframe", "1h")

    await update.effective_message.reply_text(
        f"🔎 Scan lancé sur Binance et Bybit Spot, timeframe {timeframe}…"
    )

    results, errors = await scanner.scan(timeframe)

    if not results:
        message = "Aucun signal ne passe les filtres dans l'échantillon analysé."
        if errors:
            message += "\nExchanges : " + ", ".join(errors)
        await update.effective_message.reply_text(message)
        return

    lines = [f"📈 Configurations détectées — {timeframe}"]
    for name, symbol, item in results[:15]:
        lines.append(
            f"\n{name.upper()} {symbol}\n"
            f"Prix : {item['price']:.8g}\n"
            f"RSI : {item['rsi']:.1f}\n"
            f"Volume relatif : {item['volume_ratio']:.2f}x"
        )

    await update.effective_message.reply_text("\n".join(lines))


async def backtest_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if scanner is None:
        await update.effective_message.reply_text(
            "Scanner non initialisé. Consulte les logs FadeHost."
        )
        return

    if len(context.args) != 3:
        await update.effective_message.reply_text(
            "Utilisation : /backtest binance BTC/USDT 1h"
        )
        return

    exchange, symbol, timeframe = context.args
    exchange = exchange.lower()
    symbol = symbol.upper()

    if exchange not in EXCHANGE_NAMES:
        await update.effective_message.reply_text(
            "Exchange accepté : binance ou bybit."
        )
        return

    if timeframe not in TIMEFRAMES:
        await update.effective_message.reply_text(
            "Timeframe accepté : 30m, 1h, 2h, 4h ou 12h."
        )
        return

    try:
        await update.effective_message.reply_text(
            f"⏳ Backtest {exchange} {symbol} {timeframe}…"
        )

        result = await scanner.backtest(exchange, symbol, timeframe)

        await update.effective_message.reply_text(
            f"📊 Backtest exploratoire — {exchange.upper()} {symbol} {timeframe}\n"
            f"Bougies : {result['candles']}\n"
            f"Trades simulés : {result['trades']}\n"
            f"Taux gagnants : {result['win_rate']:.1f}%\n"
            f"Rendement simulé : {result['return']:.2f}%\n"
            f"Moyenne par trade : {result['average']:.2f}%\n"
            f"Drawdown maximal : {result['drawdown']:.2f}%\n\n"
            "Simulation simplifiée avec frais estimés à 0,1 % par côté. "
            "Les performances passées ne garantissent rien."
        )

    except Exception as exc:
        log.exception("Erreur backtest")
        await update.effective_message.reply_text(
            f"❌ Backtest impossible : {exc}"
        )


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.effective_message.text or "").strip().upper()

    if "/" not in text or not text.endswith("USDT"):
        await update.effective_message.reply_text(
            "Essaie /scan, /status ou envoie BTC/USDT."
        )
        return

    symbol = text.replace(" ", "")
    if not symbol.endswith("/USDT"):
        symbol = symbol.replace("USDT", "/USDT")

    if scanner is None:
        await update.effective_message.reply_text(
            "Scanner non initialisé."
        )
        return

    exchange = next(
        (
            name for name in EXCHANGE_NAMES
            if symbol in scanner.markets.get(name, {})
        ),
        None,
    )

    if exchange is None:
        await update.effective_message.reply_text(
            f"Paire {symbol} introuvable sur Binance/Bybit Spot."
        )
        return

    timeframe = context.application.bot_data.get("timeframe", "1h")

    try:
        frame = await scanner.candles(
            exchange, symbol, timeframe, CANDLE_LIMIT
        )
        result = scanner.analyze(frame)

        await update.effective_message.reply_text(
            f"{exchange.upper()} {symbol} — {timeframe}\n"
            f"Configuration : {result['signal']}\n"
            f"Prix : {result.get('price', 0):.8g}\n"
            f"RSI : {result.get('rsi', 0):.1f}\n"
            f"Volume relatif : {result.get('volume_ratio', 0):.2f}x\n"
            f"{result['reason']}"
        )
    except Exception as exc:
        await update.effective_message.reply_text(
            f"Analyse impossible : {exc}"
        )


def main():
    if not TOKEN:
        raise SystemExit(
            "Variable TELEGRAM_BOT_TOKEN absente dans FadeHost."
        )

    app = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", start_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("tf", tf_cmd))
    app.add_handler(CommandHandler("backtest", backtest_cmd))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_router)
    )

    # run_polling gère les hooks d'initialisation et de fermeture.
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
