#!/usr/bin/env python3
"""
Запуск:
  pip install aiohttp
  python proxy.py

Затем открой crypto-arb-scanner.html в браузере.
"""

import asyncio
import json
from aiohttp import web, ClientSession, ClientTimeout

EXCHANGES = {
    "bybit":  "https://api.bybit.com/v5/market/tickers?category=spot",
    "kucoin": "https://api.kucoin.com/api/v1/market/allTickers",
    "gate":   "https://api.gateio.ws/api/v4/spot/tickers",
    "mexc":   "https://api.mexc.com/api/v3/ticker/24hr",
    "okx":    "https://www.okx.com/api/v5/market/tickers?instType=SPOT",
    "bingx":  "https://open-api.bingx.com/openApi/spot/v1/ticker/24hr",
    "bitget": "https://api.bitget.com/api/v2/spot/market/tickers",
    "htx":    "https://api.huobi.pro/market/tickers",
    "toobit": "https://api.toobit.com/quote/v1/ticker/bookTicker",
    "phemex": "https://api.phemex.com/md/spot/ticker/24hr/all",
}

# Публичные endpoints со статусом вывода монет (без авторизации)
WITHDRAW_URLS = {
    "bybit":  "https://api.bybit.com/v5/asset/coin/query-info",
    "okx":    "https://www.okx.com/api/v5/asset/currencies",
    "kucoin": "https://api.kucoin.com/api/v3/currencies",
    "gate":   "https://api.gateio.ws/api/v4/spot/currencies",
    "mexc":   "https://api.mexc.com/api/v3/capital/config/getall",
    "bitget": "https://api.bitget.com/api/v2/spot/public/coins",
    "htx":    "https://api.huobi.pro/v2/reference/currencies",
}

cache = {}
cache_ttl = {}
CACHE_SECONDS = 8

async def fetch_exchange(session, ex_id, url):
    try:
        async with session.get(url, timeout=ClientTimeout(total=8)) as resp:
            data = await resp.json(content_type=None)
            cache[ex_id] = data
            import time; cache_ttl[ex_id] = time.time()
            return data
    except Exception as e:
        print(f"[{ex_id}] ошибка: {e}")
        return None

async def handle_proxy(request):
    ex_id = request.match_info.get("exchange")
    if ex_id not in EXCHANGES:
        return web.Response(status=404, text="unknown exchange")

    import time
    now = time.time()
    if ex_id in cache and now - cache_ttl.get(ex_id, 0) < CACHE_SECONDS:
        data = cache[ex_id]
    else:
        async with ClientSession() as session:
            data = await fetch_exchange(session, ex_id, EXCHANGES[ex_id])

    if data is None:
        return web.Response(status=502, text="exchange unavailable")

    return web.Response(
        text=json.dumps(data),
        content_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )

async def handle_options(request):
    return web.Response(headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET",
        "Access-Control-Allow-Headers": "*",
    })

async def handle_index(request):
    try:
        with open("crypto-arb-scanner.html", "r", encoding="utf-8") as f:
            html = f.read()
        return web.Response(text=html, content_type="text/html")
    except FileNotFoundError:
        return web.Response(status=404, text="положи crypto-arb-scanner.html рядом с proxy.py")

wd_cache = {}
wd_cache_ttl = {}
WD_CACHE_SECONDS = 300  # статус вывода меняется редко, кэшируем 5 мин

async def handle_withdraw(request):
    ex_id = request.match_info.get("exchange")
    if ex_id not in WITHDRAW_URLS:
        return web.Response(
            text=json.dumps({}), content_type="application/json",
            headers={"Access-Control-Allow-Origin": "*"})
    now = time.time()
    if ex_id in wd_cache and now - wd_cache_ttl.get(ex_id, 0) < WD_CACHE_SECONDS:
        data = wd_cache[ex_id]
    else:
        try:
            async with ClientSession() as session:
                async with session.get(WITHDRAW_URLS[ex_id], timeout=ClientTimeout(total=8)) as resp:
                    data = await resp.json(content_type=None)
                    wd_cache[ex_id] = data
                    wd_cache_ttl[ex_id] = now
        except Exception as e:
            print(f"[{ex_id} withdraw] ошибка: {e}")
            data = {}
    return web.Response(
        text=json.dumps(data), content_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"})

app = web.Application()
app.router.add_get("/",                   handle_index)
app.router.add_get("/proxy/{exchange}",   handle_proxy)
app.router.add_get("/withdraw/{exchange}", handle_withdraw)
app.router.add_route("OPTIONS", "/withdraw/{exchange}", handle_options)
app.router.add_route("OPTIONS", "/proxy/{exchange}", handle_options)

if __name__ == "__main__":
    print("=" * 50)
    print("  ARB SCANNER ПРОКСИ")
    print("=" * 50)
    print("  Открой в браузере: http://localhost:8765")
    print("  Остановить: Ctrl+C")
    print("=" * 50)
    web.run_app(app, host="127.0.0.1", port=8765, print=None)
