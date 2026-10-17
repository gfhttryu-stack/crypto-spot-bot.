# Telegram Crypto Spot Scanner — v2 (prototype corrigé)

Bot de recherche et d'alertes uniquement pour Binance Spot et Bybit Spot. Il ne passe aucun ordre et n'a besoin d'aucune clé API de trading.

## Ce qui a été corrigé

- 7 scanners séparés : (1) volatilité croissante + prix contenu, (2) volume relatif croissant + prix contenu, (3) confluence 1+2, (4) OBV, (5) croisement EMA9/EMA21, (6) figures de bougies haussières, (7) croisement MACD haussier.
- Les signaux ne sont proposés que si la notation technique interne est BUY ou STRONG BUY. Cette notation est calculée localement; elle n'est pas la notation officielle de TradingView.
- Le mouvement du prix tient compte de la variation nette et de l'amplitude cumulée, afin d'éviter qu'une forte oscillation qui revient au point de départ soit considérée comme « stable ».
- Filtre de liquidité fondé sur le quote volume fourni par le ticker (pas de remplacement par une somme dépendant du timeframe).
- Spread manquant ou invalide = paire rejetée, et non spread artificiellement considéré comme nul.
- Bougies en cours exclues; indicateurs calculés sur les bougies clôturées disponibles.
- Les confirmations corrélées ne sont pas comptées comme des probabilités indépendantes. Le score est une note heuristique de qualité, pas un pourcentage de réussite.
- Calcul indicatif d'un stop technique et d'objectifs à partir de la structure/risk multiple; les configurations bloquées par une résistance trop proche ou ne satisfaisant pas le rendement/risque minimum sont rejetées.
- Scans manuels uniquement. Aucun scan automatique programmé.
- Envoie simplement `1` à `7` dans Telegram pour lancer le scanner correspondant.
- Envoie `BTC`, `DOT`, `CRV` ou `BTC/USDT` pour une analyse technique multi-timeframe de la paire : tendance, EMA, RSI, MACD, OBV, ATR, volume relatif, support et résistance observés. Les symboles seuls sont interprétés comme des paires cotées en USDT.

## Limites importantes

- Le bot est un prototype de recherche. Syntaxe vérifiée, mais il n'a pas été connecté aux exchanges ni testé en fonctionnement réel dans cet environnement. Les nouvelles commandes doivent être testées après déploiement.
- Les objectifs/stop sont des estimations fondées sur les données de bougies, pas des garanties. Les frais et le slippage sont affichés comme limites et ne sont pas simulés dans le signal instantané.
- Le score interne BUY/STRONG BUY n'est pas une probabilité de gain et n'est pas validé statistiquement. Aucun pourcentage de réussite réel n'est fourni sans test historique approprié.
- L'OBV est une approximation à partir des bougies, pas une mesure des achats agressifs réels. Le MACD et les EMA sont corrélés et ne constituent pas des preuves indépendantes.
- Le calcul de volume relatif compare la dernière bougie clôturée à la médiane des 20 précédentes; il ne corrige pas encore la saisonnalité intrajournalière.
- Les données de spread doivent être disponibles dans les tickers; si elles manquent, la paire est volontairement exclue. Cela peut réduire la couverture de certains marchés.
- Les scans examinent un lot rotatif de symboles par exchange afin de limiter les appels API. Un scan manuel ne garantit pas l'analyse simultanée de toutes les paires.
- Les backtests existants restent exploratoires et ne valident pas les 7 scanners. Ils ne doivent pas être utilisés comme preuve de rentabilité.

## Installation (Python 3.11+)

```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

Copier `.env.example` en `.env`, puis renseigner le token Telegram créé avec `@BotFather`. Il est recommandé de limiter l'accès avec `TELEGRAM_ALLOWED_CHAT_IDS`.

Lancer :

```bash
python bot.py
```

Commandes Telegram :
- `/start` ou `/help`
- `/status`
- `/scan` — lance un scan manuel
- `/top` — affiche les derniers résultats en mémoire
- `/strategieA`, `/strategieB`, `/strategieC` — descriptions des trois scanners principaux
- `/backtest binance ETH/USDT 1h` — test exploratoire ancien A+B, non aligné encore sur tous les nouveaux scanners

`/alerts_on` et `/alerts_off` sont conservées pour compatibilité, mais aucun scan automatique n'est planifié dans cette version.

## Paramètres (.env)

- `TELEGRAM_BOT_TOKEN` : token privé du bot
- `TELEGRAM_ALLOWED_CHAT_IDS` : identifiants autorisés séparés par virgules
- `EXCHANGES=binance,bybit`
- `TIMEFRAMES=30m,1h,2h,4h,12h`
- `MIN_QUOTE_VOLUME_USDT=250000`
- `MAX_SPREAD_PCT=0.50`
- `REL_VOLUME_MIN=2.0`
- `ALERT_SCORE_MIN=45`
- `SYMBOLS_PER_SCAN=60`
- `OHLCV_REFRESH_SECONDS=300`

N'ajoute aucune clé API d'exchange. Le bot ne passe pas d'ordres.
