#!/usr/bin/env python3
"""
ARB SCANNER — сервер

Что делает:
  • раз в 8 секунд опрашивает все биржи ОДИН раз для всех пользователей
  • считает спреды на сервере (браузер получает готовый маленький JSON)
  • вход через Google, личный кабинет, тарифы
  • свежесть данных зависит от тарифа — это и есть монетизация

Запуск:
  python server.py
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
import urllib.parse
from collections import deque
from pathlib import Path

from aiohttp import web, ClientSession, ClientTimeout

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DB_PATH = BASE_DIR / "arb.db"
CONFIG_PATH = BASE_DIR / "config.json"

PORT = int(os.environ.get("PORT", 8765))

# ─────────────────────────────────────────────────────────────────────────────
# Конфиг. Если файла нет — сайт работает, но без входа (режим "всё открыто").
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "google_client_id": "",
    "google_client_secret": "",
    "secret_key": "",
    "admin_emails": [],
    "usdt_trc20": "",
    "telegram_support": "",
    "refresh_seconds": 8,
    "free_delay_seconds": 90,
}


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_PATH.exists():
        try:
            cfg.update(json.loads(CONFIG_PATH.read_text(encoding="utf-8")))
        except Exception as e:
            print(f"[config] не читается: {e}")
    if not cfg["secret_key"]:
        cfg["secret_key"] = secrets.token_hex(32)
        try:
            CONFIG_PATH.write_text(
                json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8"
            )
        except Exception:
            pass
    return cfg


CONFIG = load_config()
AUTH_ENABLED = bool(CONFIG["google_client_id"] and CONFIG["google_client_secret"])

# ─────────────────────────────────────────────────────────────────────────────
# Тарифы
# ─────────────────────────────────────────────────────────────────────────────
FREE_EXCHANGES = {"bybit", "kucoin", "gate", "mexc", "okx"}

TIERS = {
    "free": {
        "name": "Свободный",
        "price_rub": 0,
        "max_rows": 25,
        "delay": CONFIG["free_delay_seconds"],
        "all_exchanges": False,
        "history": False,
        "api": False,
    },
    "pro": {
        "name": "Pro",
        "price_rub": 990,
        "max_rows": 500,
        "delay": 0,
        "all_exchanges": True,
        "history": True,
        "api": False,
    },
    "vip": {
        "name": "VIP",
        "price_rub": 2490,
        "max_rows": 1000,
        "delay": 0,
        "all_exchanges": True,
        "history": True,
        "api": True,
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Биржи. Все доступны из РФ, все с публичным spot-тикером.
# CORS больше не важен — запросы идёт сервер, а не браузер.
# ─────────────────────────────────────────────────────────────────────────────
EXCHANGES = {
    "bybit":    {"name": "Bybit",    "url": "https://api.bybit.com/v5/market/tickers?category=spot"},
    "kucoin":   {"name": "KuCoin",   "url": "https://api.kucoin.com/api/v1/market/allTickers"},
    "gate":     {"name": "Gate.io",  "url": "https://api.gateio.ws/api/v4/spot/tickers"},
    "mexc":     {"name": "MEXC",     "url": "https://api.mexc.com/api/v3/ticker/24hr"},
    "okx":      {"name": "OKX",      "url": "https://www.okx.com/api/v5/market/tickers?instType=SPOT"},
    "bingx":    {"name": "BingX",    "url": "https://open-api.bingx.com/openApi/spot/v1/ticker/24hr"},
    "bitget":   {"name": "Bitget",   "url": "https://api.bitget.com/api/v2/spot/market/tickers"},
    "htx":      {"name": "HTX",      "url": "https://api.huobi.pro/market/tickers"},
    "toobit":   {"name": "Toobit",   "url": "https://api.toobit.com/quote/v1/ticker/bookTicker"},
    "phemex":   {"name": "Phemex",   "url": "https://api.phemex.com/md/spot/ticker/24hr/all"},
    "coinex":   {"name": "CoinEx",   "url": "https://api.coinex.com/v2/spot/ticker"},
    "bitmart":  {"name": "BitMart",  "url": "https://api-cloud.bitmart.com/spot/quotation/v3/tickers"},
    "xt":       {"name": "XT.com",   "url": "https://sapi.xt.com/v4/public/ticker"},
    "lbank":    {"name": "LBank",    "url": "https://api.lbkex.com/v2/ticker.do?symbol=all"},
    "whitebit": {"name": "WhiteBIT", "url": "https://whitebit.com/api/v4/public/ticker"},
    "ascendex": {"name": "AscendEX", "url": "https://ascendex.com/api/pro/v1/spot/ticker"},
    "poloniex": {"name": "Poloniex", "url": "https://api.poloniex.com/markets/ticker24h"},
    "bitrue":   {"name": "Bitrue",   "url": "https://openapi.bitrue.com/api/v1/ticker/24hr"},
    "exmo":     {"name": "EXMO",     "url": "https://api.exmo.com/v1.1/ticker"},
}

# Эти биржи в массовом тикере отдают только последнюю цену, без стакана.
# Спред с ними — оценка, а не реальный ask/bid. Помечаем честно.
APPROX = {"coinex", "lbank", "whitebit"}

TRADE_URLS = {
    "bybit":    "https://www.bybit.com/trade/spot/{c}/USDT",
    "kucoin":   "https://www.kucoin.com/trade/{c}-USDT",
    "gate":     "https://www.gate.io/trade/{c}_USDT",
    "mexc":     "https://www.mexc.com/exchange/{c}_USDT",
    "okx":      "https://www.okx.com/trade-spot/{cl}-usdt",
    "bingx":    "https://bingx.com/en/spot/{c}USDT/",
    "bitget":   "https://www.bitget.com/spot/{c}USDT",
    "htx":      "https://www.htx.com/trade/{cl}_usdt/",
    "toobit":   "https://www.toobit.com/en-US/spot/{c}_USDT",
    "phemex":   "https://phemex.com/spot/trade/s{c}USDT",
    "coinex":   "https://www.coinex.com/en/exchange/{cl}-usdt",
    "bitmart":  "https://www.bitmart.com/trade/en-US?symbol={c}_USDT",
    "xt":       "https://www.xt.com/en/trade/{cl}_usdt",
    "lbank":    "https://www.lbank.com/trade/{cl}_usdt",
    "whitebit": "https://whitebit.com/trade/{c}-USDT",
    "ascendex": "https://ascendex.com/en/basic/cashtrade-spottrading/usdt/{cl}",
    "poloniex": "https://poloniex.com/trade/{c}_USDT",
    "bitrue":   "https://www.bitrue.com/trade/{cl}_usdt",
    "exmo":     "https://exmo.com/en/trade/{c}_USDT",
}


def _f(x, default=0.0):
    try:
        v = float(x)
        return v if v == v else default  # отсекаем NaN
    except (TypeError, ValueError):
        return default


def parse(ex_id: str, d):
    """Приводит ответ любой биржи к {COIN: {ask, bid, vol, chg}}."""
    out = {}
    try:
        if ex_id == "bybit":
            for t in (d.get("result") or {}).get("list") or []:
                s = t.get("symbol", "")
                if s.endswith("USDT"):
                    a, b = _f(t.get("ask1Price")), _f(t.get("bid1Price"))
                    if a > 0 and b > 0:
                        out[s[:-4]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("turnover24h")),
                                       "chg": _f(t.get("price24hPcnt")) * 100}

        elif ex_id == "kucoin":
            for t in ((d.get("data") or {}).get("ticker")) or []:
                s = t.get("symbol", "")
                if s.endswith("-USDT"):
                    a, b = _f(t.get("sell")), _f(t.get("buy"))
                    if a > 0 and b > 0:
                        out[s[:-5]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("volValue")),
                                       "chg": _f(t.get("changeRate")) * 100}

        elif ex_id == "gate":
            for t in d or []:
                s = t.get("currency_pair", "")
                if s.endswith("_USDT"):
                    a, b = _f(t.get("lowest_ask")), _f(t.get("highest_bid"))
                    if a > 0 and b > 0:
                        out[s[:-5]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("quote_volume")),
                                       "chg": _f(t.get("change_percentage"))}

        elif ex_id == "mexc":
            for t in d or []:
                s = t.get("symbol", "")
                if s.endswith("USDT"):
                    a, b = _f(t.get("askPrice")), _f(t.get("bidPrice"))
                    if a > 0 and b > 0:
                        out[s[:-4]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("quoteVolume")),
                                       "chg": _f(t.get("priceChangePercent"))}

        elif ex_id == "okx":
            for t in d.get("data") or []:
                s = t.get("instId", "")
                if s.endswith("-USDT"):
                    a, b = _f(t.get("askPx")), _f(t.get("bidPx"))
                    last, op = _f(t.get("last")), _f(t.get("open24h"))
                    if a > 0 and b > 0:
                        out[s[:-5]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("volCcy24h")),
                                       "chg": ((last - op) / op * 100) if op else 0}

        elif ex_id == "bingx":
            for t in d.get("data") or []:
                s = t.get("symbol", "")
                if s.endswith("-USDT"):
                    a, b = _f(t.get("askPrice")), _f(t.get("bidPrice"))
                    if a > 0 and b > 0:
                        out[s[:-5]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("quoteVolume")),
                                       "chg": _f(t.get("priceChangePercent"))}

        elif ex_id == "bitget":
            for t in d.get("data") or []:
                s = t.get("symbol", "")
                if s.endswith("USDT"):
                    a, b = _f(t.get("askPr")), _f(t.get("bidPr"))
                    if a > 0 and b > 0:
                        out[s[:-4]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("usdtVolume") or t.get("usdtVol")),
                                       "chg": _f(t.get("changeUtc24h")) * 100}

        elif ex_id == "htx":
            for t in d.get("data") or []:
                s = t.get("symbol", "")
                if s.endswith("usdt"):
                    a, b = _f(t.get("ask")), _f(t.get("bid"))
                    close, op = _f(t.get("close")), _f(t.get("open"))
                    if a > 0 and b > 0:
                        out[s[:-4].upper()] = {"ask": a, "bid": b,
                                               "vol": _f(t.get("vol")),
                                               "chg": ((close - op) / op * 100) if op else 0}

        elif ex_id == "toobit":
            rows = d if isinstance(d, list) else (d.get("data") or d.get("result") or [])
            for t in rows:
                s = t.get("s") or t.get("symbol") or ""
                if s.endswith("USDT"):
                    a = _f(t.get("ap") or t.get("askPrice") or t.get("a"))
                    b = _f(t.get("bp") or t.get("bidPrice") or t.get("b"))
                    if a > 0 and b > 0:
                        out[s[:-4]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("qv")), "chg": _f(t.get("p"))}

        elif ex_id == "phemex":
            rows = (d.get("result") or {}).get("list") or d.get("data") or []
            for t in rows:
                s = t.get("symbol", "")
                if s.startswith("s") and s.endswith("USDT"):
                    a = _f(t.get("askEp")) / 1e8 if t.get("askEp") is not None else _f(t.get("ask"))
                    b = _f(t.get("bidEp")) / 1e8 if t.get("bidEp") is not None else _f(t.get("bid"))
                    if a > 0 and b > 0 and b <= a * 1.5:
                        out[s[1:-4]] = {"ask": a, "bid": b,
                                        "vol": _f(t.get("turnoverEv")) / 1e8,
                                        "chg": _f(t.get("priceChgPct")) * 100}

        elif ex_id == "coinex":
            for t in d.get("data") or []:
                s = t.get("market", "")
                if s.endswith("USDT"):
                    last = _f(t.get("last"))
                    if last > 0:
                        out[s[:-4]] = {"ask": last, "bid": last,
                                       "vol": _f(t.get("value")),
                                       "chg": _f(t.get("change_rate")) * 100}

        elif ex_id == "bitmart":
            for row in d.get("data") or []:
                # [symbol, last, v24, qv24, open24, high24, low24, fluct, bid, bidSz, ask, askSz, ts]
                if not isinstance(row, list) or len(row) < 12:
                    continue
                s = row[0]
                if s.endswith("_USDT"):
                    b, a = _f(row[8]), _f(row[10])
                    if a > 0 and b > 0:
                        out[s[:-5]] = {"ask": a, "bid": b,
                                       "vol": _f(row[3]), "chg": _f(row[7]) * 100}

        elif ex_id == "xt":
            for t in (d.get("result") or d.get("data") or []):
                s = (t.get("s") or "").upper()
                if s.endswith("_USDT"):
                    a, b = _f(t.get("a") or t.get("ap")), _f(t.get("b") or t.get("bp"))
                    if a <= 0 or b <= 0:
                        a = b = _f(t.get("c"))
                    if a > 0 and b > 0:
                        out[s[:-5]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("q")), "chg": _f(t.get("cr")) * 100}

        elif ex_id == "lbank":
            for t in d.get("data") or []:
                s = (t.get("symbol") or "").lower()
                tk = t.get("ticker") or {}
                if s.endswith("_usdt"):
                    last = _f(tk.get("latest"))
                    if last > 0:
                        out[s[:-5].upper()] = {"ask": last, "bid": last,
                                               "vol": _f(tk.get("turnover")),
                                               "chg": _f(tk.get("change"))}

        elif ex_id == "whitebit":
            for s, t in (d or {}).items():
                if s.endswith("_USDT"):
                    last = _f(t.get("last_price"))
                    if last > 0:
                        out[s[:-5]] = {"ask": last, "bid": last,
                                       "vol": _f(t.get("quote_volume")),
                                       "chg": _f(t.get("change"))}

        elif ex_id == "ascendex":
            for t in d.get("data") or []:
                s = t.get("symbol", "")
                if s.endswith("/USDT"):
                    ask = t.get("ask") or []
                    bid = t.get("bid") or []
                    a = _f(ask[0]) if ask else 0
                    b = _f(bid[0]) if bid else 0
                    if a > 0 and b > 0:
                        out[s[:-5]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("volume")) * a, "chg": 0}

        elif ex_id == "poloniex":
            for t in d or []:
                s = t.get("symbol", "")
                if s.endswith("_USDT"):
                    a, b = _f(t.get("ask")), _f(t.get("bid"))
                    if a > 0 and b > 0:
                        out[s[:-5]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("amount")),
                                       "chg": _f(t.get("dailyChange")) * 100}

        elif ex_id == "bitrue":
            for t in d or []:
                s = t.get("symbol", "")
                if s.endswith("USDT"):
                    a, b = _f(t.get("askPrice")), _f(t.get("bidPrice"))
                    if a <= 0 or b <= 0:
                        a = b = _f(t.get("lastPrice"))
                    if a > 0 and b > 0:
                        out[s[:-4]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("quoteVolume")),
                                       "chg": _f(t.get("priceChangePercent"))}

        elif ex_id == "exmo":
            for s, t in (d or {}).items():
                if s.endswith("_USDT"):
                    a, b = _f(t.get("sell_price")), _f(t.get("buy_price"))
                    if a > 0 and b > 0:
                        out[s[:-5]] = {"ask": a, "bid": b,
                                       "vol": _f(t.get("vol_curr")), "chg": 0}
    except Exception as e:
        print(f"[{ex_id}] парсер упал: {e}")
        return {}

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Движок спредов
# ─────────────────────────────────────────────────────────────────────────────
MIN_SPREAD = 0.15
MAX_SPREAD = 50.0
MIN_VOLUME = 10_000

STATE = {
    "status": {k: "wait" for k in EXCHANGES},   # ok | err | wait
    "snapshots": deque(maxlen=40),              # [(ts, payload)]
    "pairs_checked": 0,
    "last_update": 0,
}


async def fetch_one(session: ClientSession, ex_id: str):
    url = EXCHANGES[ex_id]["url"]
    try:
        async with session.get(url, timeout=ClientTimeout(total=9)) as r:
            data = await r.json(content_type=None)
        parsed = parse(ex_id, data)
        if len(parsed) > 3:
            STATE["status"][ex_id] = "ok"
            return ex_id, parsed
        STATE["status"][ex_id] = "err"
    except Exception as e:
        STATE["status"][ex_id] = "err"
        print(f"[{ex_id}] {type(e).__name__}: {e}")
    return ex_id, {}


def build_spreads(prices: dict):
    """Считаем все спреды один раз для всех пользователей."""
    coin_map = {}
    for ex_id, tickers in prices.items():
        for coin, t in tickers.items():
            coin_map.setdefault(coin, []).append(ex_id)

    spreads = []
    pairs = 0

    for coin, ex_list in coin_map.items():
        if len(ex_list) < 2:
            continue
        pairs += 1

        max_vol = max(prices[e][coin].get("vol", 0) for e in ex_list)
        if 0 < max_vol < MIN_VOLUME:
            continue

        for i in range(len(ex_list)):
            for j in range(len(ex_list)):
                if i == j:
                    continue
                ea, eb = ex_list[i], ex_list[j]
                pa, pb = prices[ea][coin], prices[eb][coin]
                if pa["ask"] <= 0 or pb["bid"] <= 0:
                    continue
                sp = (pb["bid"] - pa["ask"]) / pa["ask"] * 100
                if MIN_SPREAD < sp < MAX_SPREAD:
                    spreads.append({
                        "c": coin,
                        "be": ea, "se": eb,
                        "bp": pa["ask"], "sp": pb["bid"],
                        "s": round(sp, 3),
                        "bv": round(pa.get("vol", 0)),
                        "sv": round(pb.get("vol", 0)),
                        "ch": round(pa.get("chg", 0) or pb.get("chg", 0), 2),
                    })

    spreads.sort(key=lambda x: -x["s"])

    # Все маршруты по монете — для всплывающей подсказки
    routes = {}
    for s in spreads:
        routes.setdefault(s["c"], []).append([s["be"], s["se"], s["s"]])
    routes = {k: v[:8] for k, v in routes.items()}

    # Уникальные монеты — лучший спред на монету
    seen, best = set(), []
    for s in spreads:
        if s["c"] not in seen:
            seen.add(s["c"])
            best.append(s)

    return {
        "spreads": best,
        "routes": routes,
        "pairs": pairs,
        "hot": sum(1 for s in best if s["s"] >= 1),
        "ts": int(time.time()),
    }


async def refresher(app):
    """Фоновая задача: опрашивает биржи и складывает снимки."""
    await asyncio.sleep(1)
    async with ClientSession(headers={"User-Agent": "arb-scanner/2.0"}) as session:
        while True:
            t0 = time.time()
            try:
                results = await asyncio.gather(
                    *[fetch_one(session, e) for e in EXCHANGES],
                    return_exceptions=True,
                )
                prices = {}
                for r in results:
                    if isinstance(r, tuple) and r[1]:
                        prices[r[0]] = r[1]

                if prices:
                    payload = build_spreads(prices)
                    payload["online"] = sum(
                        1 for v in STATE["status"].values() if v == "ok"
                    )
                    STATE["snapshots"].append((time.time(), payload))
                    STATE["pairs_checked"] = payload["pairs"]
                    STATE["last_update"] = payload["ts"]
                    print(
                        f"[scan] {len(prices)} бирж · {len(payload['spreads'])} спредов "
                        f"· {time.time() - t0:.1f}s"
                    )
            except Exception as e:
                print(f"[refresher] {type(e).__name__}: {e}")

            await asyncio.sleep(max(2, CONFIG["refresh_seconds"] - (time.time() - t0)))


def snapshot_for(delay: int):
    """Свежий снимок для платных, задержанный — для бесплатных."""
    if not STATE["snapshots"]:
        return None
    if delay <= 0:
        return STATE["snapshots"][-1][1]
    target = time.time() - delay
    chosen = STATE["snapshots"][0][1]
    for ts, payload in STATE["snapshots"]:
        if ts <= target:
            chosen = payload
        else:
            break
    return chosen


# ─────────────────────────────────────────────────────────────────────────────
# База
# ─────────────────────────────────────────────────────────────────────────────
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            email      TEXT UNIQUE NOT NULL,
            name       TEXT,
            picture    TEXT,
            tier       TEXT DEFAULT 'free',
            tier_until INTEGER DEFAULT 0,
            created_at INTEGER,
            last_seen  INTEGER
        );
        CREATE TABLE IF NOT EXISTS payments (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            plan       TEXT,
            months     INTEGER DEFAULT 1,
            amount     REAL,
            currency   TEXT,
            status     TEXT DEFAULT 'pending',
            txid       TEXT,
            created_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS favorites (
            user_id INTEGER NOT NULL,
            coin    TEXT NOT NULL,
            PRIMARY KEY (user_id, coin)
        );
        """)


def user_tier(u) -> str:
    if not u:
        return "free"
    if u["tier"] in ("pro", "vip") and u["tier_until"] > time.time():
        return u["tier"]
    return "free"


# ─────────────────────────────────────────────────────────────────────────────
# Сессии — подписанная кука, без внешних зависимостей
# ─────────────────────────────────────────────────────────────────────────────
def sign(data: str) -> str:
    sig = hmac.new(CONFIG["secret_key"].encode(), data.encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode().rstrip("=")


def make_token(user_id: int) -> str:
    body = f"{user_id}.{int(time.time())}"
    return f"{body}.{sign(body)}"


def read_token(token: str):
    try:
        uid, ts, sig = token.rsplit(".", 2)
        body = f"{uid}.{ts}"
        if not hmac.compare_digest(sig, sign(body)):
            return None
        if time.time() - int(ts) > 60 * 60 * 24 * 30:
            return None
        return int(uid)
    except Exception:
        return None


def current_user(request):
    token = request.cookies.get("sid")
    if not token:
        return None
    uid = read_token(token)
    if not uid:
        return None
    with db() as c:
        row = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    return row


# ─────────────────────────────────────────────────────────────────────────────
# Страницы
# ─────────────────────────────────────────────────────────────────────────────
def page(name: str):
    f = STATIC_DIR / name
    if not f.exists():
        return web.Response(status=404, text=f"нет файла static/{name}")
    return web.Response(text=f.read_text(encoding="utf-8"), content_type="text/html")


async def h_landing(request):
    if current_user(request) or not AUTH_ENABLED:
        raise web.HTTPFound("/app")
    return page("landing.html")


async def h_app(request):
    if AUTH_ENABLED and not current_user(request):
        raise web.HTTPFound("/")
    return page("app.html")


async def h_cabinet(request):
    if AUTH_ENABLED and not current_user(request):
        raise web.HTTPFound("/")
    return page("cabinet.html")


# ─────────────────────────────────────────────────────────────────────────────
# Вход через Google
# ─────────────────────────────────────────────────────────────────────────────
def base_url(request) -> str:
    proto = request.headers.get("X-Forwarded-Proto", "http")
    host = request.headers.get("X-Forwarded-Host") or request.host
    return f"{proto}://{host}"


async def h_google_start(request):
    if not AUTH_ENABLED:
        raise web.HTTPFound("/app")
    state = secrets.token_urlsafe(16)
    params = {
        "client_id": CONFIG["google_client_id"],
        "redirect_uri": f"{base_url(request)}/auth/google/callback",
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "prompt": "select_account",
    }
    resp = web.HTTPFound(
        "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    )
    resp.set_cookie("oauth_state", state, httponly=True, max_age=600, samesite="Lax")
    raise resp


async def h_google_callback(request):
    code = request.query.get("code")
    state = request.query.get("state")
    if not code or state != request.cookies.get("oauth_state"):
        raise web.HTTPFound("/?error=auth")

    async with ClientSession() as s:
        async with s.post("https://oauth2.googleapis.com/token", data={
            "code": code,
            "client_id": CONFIG["google_client_id"],
            "client_secret": CONFIG["google_client_secret"],
            "redirect_uri": f"{base_url(request)}/auth/google/callback",
            "grant_type": "authorization_code",
        }, timeout=ClientTimeout(total=15)) as r:
            tok = await r.json()

        access = tok.get("access_token")
        if not access:
            raise web.HTTPFound("/?error=token")

        async with s.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access}"},
            timeout=ClientTimeout(total=15),
        ) as r:
            info = await r.json()

    email = (info.get("email") or "").lower()
    if not email:
        raise web.HTTPFound("/?error=email")

    now = int(time.time())
    with db() as c:
        row = c.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
        if row:
            c.execute("UPDATE users SET last_seen=?, name=?, picture=? WHERE id=?",
                      (now, info.get("name"), info.get("picture"), row["id"]))
            uid = row["id"]
        else:
            cur = c.execute(
                "INSERT INTO users (email,name,picture,tier,created_at,last_seen) "
                "VALUES (?,?,?,'free',?,?)",
                (email, info.get("name"), info.get("picture"), now, now))
            uid = cur.lastrowid

    resp = web.HTTPFound("/app")
    resp.set_cookie("sid", make_token(uid), httponly=True,
                    max_age=60 * 60 * 24 * 30, samesite="Lax", path="/")
    resp.del_cookie("oauth_state")
    raise resp


async def h_logout(request):
    resp = web.HTTPFound("/")
    resp.del_cookie("sid", path="/")
    raise resp


# ─────────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────────
def jr(data, status=200):
    return web.json_response(data, status=status, dumps=lambda o: json.dumps(o, separators=(",", ":")))


async def h_api_me(request):
    u = current_user(request)
    tier = user_tier(u)
    return jr({
        "auth_enabled": AUTH_ENABLED,
        "logged_in": bool(u),
        "email": u["email"] if u else None,
        "name": u["name"] if u else None,
        "picture": u["picture"] if u else None,
        "tier": tier,
        "tier_name": TIERS[tier]["name"],
        "tier_until": u["tier_until"] if u else 0,
        "limits": TIERS[tier],
        "exchanges": {k: v["name"] for k, v in EXCHANGES.items()},
        "trade_urls": TRADE_URLS,
        "free_exchanges": sorted(FREE_EXCHANGES),
        "approx": sorted(APPROX),
    })


async def h_api_spreads(request):
    u = current_user(request)
    tier = user_tier(u) if AUTH_ENABLED else "vip"
    lim = TIERS[tier]

    snap = snapshot_for(lim["delay"])
    if not snap:
        return jr({"loading": True, "spreads": [], "status": STATE["status"]})

    rows = snap["spreads"]
    if not lim["all_exchanges"]:
        rows = [r for r in rows if r["be"] in FREE_EXCHANGES and r["se"] in FREE_EXCHANGES]

    total_before_cut = len(rows)
    rows = rows[: lim["max_rows"]]

    return jr({
        "spreads": rows,
        "routes": snap["routes"],
        "status": STATE["status"],
        "pairs": snap["pairs"],
        "hot": snap["hot"],
        "online": snap.get("online", 0),
        "ts": snap["ts"],
        "age": int(time.time() - snap["ts"]),
        "tier": tier,
        "truncated": max(0, total_before_cut - len(rows)),
        "delay": lim["delay"],
    })


async def h_api_favorites(request):
    u = current_user(request)
    if not u:
        return jr({"favorites": []})
    if request.method == "GET":
        with db() as c:
            rows = c.execute("SELECT coin FROM favorites WHERE user_id=?", (u["id"],)).fetchall()
        return jr({"favorites": [r["coin"] for r in rows]})

    body = await request.json()
    coin = (body.get("coin") or "").upper()[:20]
    if not coin:
        return jr({"ok": False}, 400)
    with db() as c:
        if body.get("add"):
            c.execute("INSERT OR IGNORE INTO favorites (user_id,coin) VALUES (?,?)", (u["id"], coin))
        else:
            c.execute("DELETE FROM favorites WHERE user_id=? AND coin=?", (u["id"], coin))
    return jr({"ok": True})


async def h_api_plans(request):
    return jr({
        "plans": [
            {"id": k, **v} for k, v in TIERS.items()
        ],
        "usdt_trc20": CONFIG["usdt_trc20"],
        "telegram_support": CONFIG["telegram_support"],
    })


async def h_api_order(request):
    """Создаёт заявку на оплату. Подтверждает админ или платёжный провайдер."""
    u = current_user(request)
    if not u:
        return jr({"ok": False, "error": "нужен вход"}, 401)
    body = await request.json()
    plan = body.get("plan")
    months = max(1, min(12, int(body.get("months", 1))))
    if plan not in ("pro", "vip"):
        return jr({"ok": False, "error": "нет такого тарифа"}, 400)

    amount = TIERS[plan]["price_rub"] * months
    with db() as c:
        cur = c.execute(
            "INSERT INTO payments (user_id,plan,months,amount,currency,status,created_at) "
            "VALUES (?,?,?,?,'RUB','pending',?)",
            (u["id"], plan, months, amount, int(time.time())))
        oid = cur.lastrowid

    return jr({
        "ok": True,
        "order_id": oid,
        "amount_rub": amount,
        "usdt_trc20": CONFIG["usdt_trc20"],
        "telegram_support": CONFIG["telegram_support"],
    })


# ─────────────────────────────────────────────────────────────────────────────
# Админ: выдать тариф вручную после проверки платежа
# ─────────────────────────────────────────────────────────────────────────────
def is_admin(u) -> bool:
    return bool(u) and u["email"] in [e.lower() for e in CONFIG["admin_emails"]]


async def h_admin_grant(request):
    u = current_user(request)
    if not is_admin(u):
        return jr({"ok": False}, 403)
    body = await request.json()
    email = (body.get("email") or "").lower()
    plan = body.get("plan", "pro")
    months = max(1, min(24, int(body.get("months", 1))))
    until = int(time.time()) + months * 30 * 24 * 3600
    with db() as c:
        r = c.execute("UPDATE users SET tier=?, tier_until=? WHERE email=?",
                      (plan, until, email))
    return jr({"ok": r.rowcount > 0, "until": until})


async def h_admin_users(request):
    u = current_user(request)
    if not is_admin(u):
        return jr({"ok": False}, 403)
    with db() as c:
        users = [dict(r) for r in c.execute(
            "SELECT id,email,name,tier,tier_until,created_at,last_seen "
            "FROM users ORDER BY id DESC LIMIT 200").fetchall()]
        pays = [dict(r) for r in c.execute(
            "SELECT * FROM payments ORDER BY id DESC LIMIT 100").fetchall()]
    return jr({"users": users, "payments": pays})


# ─────────────────────────────────────────────────────────────────────────────
# Публичная витрина для лендинга — только верхушка, без входа
# ─────────────────────────────────────────────────────────────────────────────
async def h_api_teaser(request):
    snap = snapshot_for(CONFIG["free_delay_seconds"])
    if not snap:
        return jr({"rows": [], "online": 0, "pairs": 0})
    return jr({
        "rows": [
            {"c": s["c"], "be": EXCHANGES[s["be"]]["name"],
             "se": EXCHANGES[s["se"]]["name"], "s": s["s"]}
            for s in snap["spreads"][:12]
        ],
        "online": snap.get("online", 0),
        "pairs": snap["pairs"],
        "total": len(snap["spreads"]),
        "exchanges": len(EXCHANGES),
    })


# ─────────────────────────────────────────────────────────────────────────────
# Совместимость со старой версией
# ─────────────────────────────────────────────────────────────────────────────
async def h_legacy_proxy(request):
    ex = request.match_info.get("exchange")
    if ex not in EXCHANGES:
        return web.Response(status=404, text="unknown exchange")
    async with ClientSession() as s:
        try:
            async with s.get(EXCHANGES[ex]["url"], timeout=ClientTimeout(total=9)) as r:
                data = await r.json(content_type=None)
        except Exception:
            return web.Response(status=502, text="exchange unavailable")
    return web.json_response(data, headers={"Access-Control-Allow-Origin": "*"})


async def on_start(app):
    app["refresher"] = asyncio.create_task(refresher(app))


async def on_cleanup(app):
    app["refresher"].cancel()


def build_app():
    init_db()
    app = web.Application()
    app.add_routes([
        web.get("/", h_landing),
        web.get("/app", h_app),
        web.get("/cabinet", h_cabinet),

        web.get("/auth/google", h_google_start),
        web.get("/auth/google/callback", h_google_callback),
        web.get("/logout", h_logout),

        web.get("/api/me", h_api_me),
        web.get("/api/spreads", h_api_spreads),
        web.get("/api/teaser", h_api_teaser),
        web.get("/api/plans", h_api_plans),
        web.get("/api/favorites", h_api_favorites),
        web.post("/api/favorites", h_api_favorites),
        web.post("/api/order", h_api_order),

        web.get("/api/admin/users", h_admin_users),
        web.post("/api/admin/grant", h_admin_grant),

        web.get("/proxy/{exchange}", h_legacy_proxy),
    ])
    if STATIC_DIR.exists():
        app.router.add_static("/static/", STATIC_DIR)
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    print("=" * 56)
    print("  ARB SCANNER")
    print(f"  Биржи: {len(EXCHANGES)} · обновление раз в {CONFIG['refresh_seconds']}с")
    print(f"  Вход через Google: {'включён' if AUTH_ENABLED else 'ВЫКЛЮЧЕН (нет ключей)'}")
    print(f"  http://127.0.0.1:{PORT}")
    print("=" * 56)
    web.run_app(build_app(), host="127.0.0.1", port=PORT, print=None)
