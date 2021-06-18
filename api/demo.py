import asyncio
from datetime import datetime, timezone
from enum import Enum
from functools import partial
import os
from typing import Dict, List

from binance import AsyncClient, BinanceSocketManager, enums
from fastapi import FastAPI
from fastapi import Request
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
from pydantic import BaseModel
from starlette.websockets import WebSocketState
from websockets.exceptions import ConnectionClosedOK


SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "DOGEUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "ADAUSDT",
    "DOTUSDT",
    "MATICUSDT",
    "EOSUSDT",
    "LINKUSDT",
]


def get_utc_timestamp(iso_format_datetime: str):
    return int(
        datetime.fromisoformat(iso_format_datetime)
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1000
    )


def get_utc_timestamp_now():
    return int(
        datetime.now(timezone.utc).replace(tzinfo=timezone.utc).timestamp() * 1000
    )


def get_times(start_time: str, end_time: str):
    """
    Return utc timestamps from isoformat datetime strings.
    """

    if start_time is not None:
        start_time = get_utc_timestamp(start_time)

    if end_time is None:
        end_time = get_utc_timestamp_now()
    else:
        end_time = min(get_utc_timestamp_now(), get_utc_timestamp(end_time))

    return start_time, end_time


origins = [
    "http://localhost:3000",
]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

candle_keys = ["time", "open", "high", "low", "close", "volume"]
kline_columns = candle_keys + [
    "close_time",
    "quote_volume",
    "n_trades",
    "buy_base_volume",
    "buy_quote_volume",
    "ignore",
]

async_client = partial(
    AsyncClient.create,
    api_key=os.getenv("BINANCE_API_KEY"),
    api_secret=os.getenv("BINANCE_API_SECRET"),
)


class marketName(str, Enum):
    futures = "futures"
    spot = "spot"
    margin = "margin"


@app.get("/")
def read_root(request: Request):
    return templates.TemplateResponse("index.htm", {"request": request})


@app.get("/exchange")
async def get_exhange_info():
    client = await async_client()

    async def get_futures_exchange_info():
        symbol_info = await client.futures_exchange_info()
        return [s for s in symbol_info["symbols"] if s["symbol"] == SYMBOLS[0]]

    async def get_exchange_info():
        symbol_info = await client.get_exchange_info()
        return [s for s in symbol_info["symbols"] if s["symbol"] == SYMBOLS[0]]

    res = await asyncio.gather(get_futures_exchange_info(), get_exchange_info())

    return res


@app.get("/account")
async def get_account():
    client = await async_client()

    res = await asyncio.gather(
        client.futures_position_information(), client.get_margin_account()
    )

    margin_positions = {
        p["asset"]: {"netAsset": p["netAsset"]}
        for p in res[1]["userAssets"]
        if f"{p['asset']}USDT" in SYMBOLS
    }

    positions = []
    for p in res[0]:
        if p["symbol"] in SYMBOLS:
            p["marginPositionAmt"] = margin_positions[p["symbol"].replace("USDT", "")][
                "netAsset"
            ]
            positions.append(p)

    return sorted(positions, key=lambda p: SYMBOLS.index(p["symbol"]))


@app.get("/wallet")
async def get_account():
    client = await async_client()

    balances = {a: {} for a in ["USDT", "BUSD", "BNB"]}
    res = await asyncio.gather(client.futures_account(), client.get_margin_account())

    for futures_asset in res[0]["assets"]:
        if futures_asset["asset"] in balances:
            balances[futures_asset["asset"]]["futuresPositionAmt"] = futures_asset[
                "availableBalance"
            ]

    for margin_asset in res[1]["userAssets"]:
        if margin_asset["asset"] in balances:
            balances[margin_asset["asset"]]["marginPositionAmt"] = margin_asset[
                "netAsset"
            ]
            balances[margin_asset["asset"]]["totalPositionAmt"] = str(
                float(balances[margin_asset["asset"]]["futuresPositionAmt"])
                + float(balances[margin_asset["asset"]]["marginPositionAmt"])
            )

    assets = [{"asset": b, **balances[b]} for b in balances]

    return assets


@app.get("/income-history")
async def get_income_history():
    client = await async_client()

    res = await client.futures_income_history()

    return res


@app.get("/trades")
async def get_trades():
    client = await async_client()

    res = await client.futures_account_trades()

    df = pd.DataFrame(res).astype({"qty": float}).sort_values(["symbol", "time"])
    df.qty = df.qty * ((df.side == "SELL") * -1 + (df.side == "BUY"))
    df["qty_cumsum"] = df.groupby(["symbol"]).cumsum().qty

    return df.to_dict(orient="records")


@app.get("/trades/margin/{symbol}")
async def get_trades_margin(symbol: str):
    client = await async_client()

    res = await client.get_margin_trades(symbol=symbol)

    return res


class Position(BaseModel):
    symbol: str
    futuresQty: float
    marginQty: float


@app.post("/close-positions")
async def get_trades(position: Position):
    client = await async_client()

    futures_params = {
        "symbol": position.symbol,
        "quantity": abs(position.futuresQty),
        "side": "BUY" if position.futuresQty < 0 else "SELL",
        "type": "MARKET",
    }

    margin_params = {
        "symbol": position.symbol,
        "quantity": round(abs(position.marginQty), 6),
        "side": "BUY" if position.marginQty < 0 else "SELL",
        "type": "MARKET",
    }

    res = await asyncio.gather(
        client.create_margin_order(**margin_params),
        # client.futures_create_order(**futures_params),
    )

    return res

    # return margin_params


@app.get("/klines/{market}/{symbol}")
async def get_kline_history(market: marketName, symbol: str):
    client = await async_client()

    methods = {
        marketName.futures: client.futures_klines,
        marketName.spot: client.get_klines,
    }

    res = await methods[market](symbol=symbol, interval=client.KLINE_INTERVAL_1MINUTE)

    processed_klines = [
        {key: value for key, value in zip(candle_keys, kline)} for kline in res
    ]

    for k in processed_klines:
        k["time"] = k["time"] / 1000

    return processed_klines


@app.get("/funding/{symbol}/")
async def get_spread_history(
    symbol: str, start_time: str = None, end_time: str = None, limit: int = 1000
):
    client = await async_client()

    # start_time, end_time = get_times(start_time, end_time)
    # startTime=start_time, endTime=end_time,

    res = await client.futures_funding_rate(symbol=symbol, limit=limit)

    processed_rates = [
        {"time": r["fundingTime"] / 1000, "value": r["fundingRate"]} for r in res
    ]

    return processed_rates


@app.get("/spread/{symbol}")
async def get_spread_history(symbol: str):
    client = await async_client()

    methods = [client.futures_klines, client.get_klines]
    methods = [
        method(symbol=symbol, interval=client.KLINE_INTERVAL_1MINUTE)
        for method in methods
    ]

    res = await asyncio.gather(*methods)
    dfs = [
        pd.DataFrame(r, columns=kline_columns, dtype=float).set_index("time")
        for r in res
    ]

    assert all(dfs[0].index == dfs[1].index)

    df_processed = pd.DataFrame({"time": dfs[0].index.values}).set_index("time")

    for key in ["open", "close"]:
        df_processed[key] = dfs[0][key] / dfs[1][key] - 1

    df_processed["high"] = df_processed[["open", "close"]].max(axis=1)
    df_processed["low"] = df_processed[["open", "close"]].min(axis=1)

    df_processed["volume"] = pd.concat([dfs[i]["volume"] for i in [0, 1]], axis=1).min(
        axis=1
    )

    df_processed.index /= 1000

    return df_processed.reset_index().to_dict(orient="records")


@app.websocket("/klines/futures/{symbol}")
async def get_klines_stream_futures(websocket: WebSocket, symbol: str):
    await websocket.accept()

    client = await async_client()
    bm = BinanceSocketManager(client)
    cm = bm._get_futures_socket(
        path=f"{symbol.lower()}@kline_1m", futures_type=enums.FuturesType.USD_M
    )

    async with cm as stream:
        while True:
            res = await stream.recv()
            kline = res["data"]["k"]
            processed_kline = {key: kline[key[0]] for key in candle_keys}
            processed_kline["time"] /= 1000
            await websocket.send_json(processed_kline)


@app.websocket("/klines/spot/{symbol}")
async def get_klines_stream_spot(websocket: WebSocket, symbol: str):
    await websocket.accept()

    client = await async_client()
    bm = BinanceSocketManager(client)
    cm = bm.kline_socket(symbol)

    async with cm as stream:
        while True:
            res = await stream.recv()
            kline = res["k"]
            processed_kline = {key: kline[key[0]] for key in candle_keys}
            processed_kline["time"] /= 1000
            await websocket.send_json(processed_kline)


@app.websocket("/spread/{symbol}")
async def get_spread_stream(websocket: WebSocket, symbol: str):
    """
    This effectively combines the futures and spot websocket streams from
    different endpoints and calculates the spread between them as a candlestick.
    The futures websocket streams about five times as fast as the spot websocket
    so the data sent from this endpoint goes out from the async function for the
    futures websocket. The async function for the spot websocket updates and global
    variable with the most recent spot candlestick, which is then used in the futures
    async function to calculate the spread candlestick.
    """

    await websocket.accept()
    client = await async_client()
    kline_spots = [{}]

    async def futures_kline_listener(client):
        bm = BinanceSocketManager(client)
        async with bm._get_futures_socket(
            path=f"{symbol.lower()}@kline_1m", futures_type=enums.FuturesType.USD_M
        ) as stream:
            old_processed_kline = None
            while True:
                res = await stream.recv()
                kline_futures = res["data"]["k"]

                if kline_spots[0]:

                    kline_spot = kline_spots[0]
                    processed_kline = {
                        "time": kline_futures["t"] / 1000,
                        "open": float(kline_futures["o"]) / float(kline_spot["o"]) - 1,
                        "high": float(kline_futures["c"]) / float(kline_spot["c"]) - 1,
                        "low": float(kline_futures["c"]) / float(kline_spot["c"]) - 1,
                        "close": float(kline_futures["c"]) / float(kline_spot["c"]) - 1,
                        "volume": min(
                            float(kline_futures["v"]), float(kline_spot["v"])
                        ),
                        "trade_time": res["data"]["E"] / 1000,
                    }

                    if (
                        old_processed_kline
                        and old_processed_kline["open"] == processed_kline["open"]
                    ):
                        processed_kline["high"] = max(
                            processed_kline["close"], old_processed_kline["high"]
                        )
                        processed_kline["low"] = min(
                            processed_kline["close"], old_processed_kline["low"]
                        )

                    old_processed_kline = processed_kline

                    await websocket.send_json(processed_kline)

    async def spot_kline_listener(client):
        bm = BinanceSocketManager(client)
        async with bm.kline_socket(symbol) as stream:
            while True:
                res = await stream.recv()
                kline_spots[0] = res["k"]

    res = await asyncio.gather(
        futures_kline_listener(client), spot_kline_listener(client)
    )


@app.websocket("/market-stream")
async def get_market_stream(websocket: WebSocket):
    """ """

    await websocket.accept()
    client = await async_client()
    spot_prices = [{}]

    async def futures_market_stream(client):
        bm = BinanceSocketManager(client)
        async with bm._get_futures_socket(
            path=f"!markPrice@arr@1s", futures_type=enums.FuturesType.USD_M
        ) as stream:
            while websocket.client_state == WebSocketState.CONNECTED:
                res = await stream.recv()
                res = [r for r in res["data"] if r["s"] in SYMBOLS]
                res = sorted(res, key=lambda r: SYMBOLS.index(r["s"]))

                if len(spot_prices[0]) == len(SYMBOLS):
                    for r in res:
                        r["spotPrice"] = spot_prices[0][r["s"]]
                        r["spread"] = float(r["p"]) / float(r["spotPrice"]) - 1
                    await websocket.send_json(res)

    async def spot_market_stream(client):
        bm = BinanceSocketManager(client)
        async with bm.multiplex_socket(
            [f"{s.lower()}@ticker" for s in SYMBOLS]
        ) as stream:
            while True:
                res = await stream.recv()
                symbol = res["stream"].split("@")[0].upper()
                spot_prices[0][symbol] = res["data"]["c"]

    res = await asyncio.gather(
        futures_market_stream(client), spot_market_stream(client)
    )


@app.websocket("/user/spot")
async def get_user_stream_spot(websocket: WebSocket):
    await websocket.accept()

    try:
        while websocket.client_state == WebSocketState.CONNECTED:
            await asyncio.sleep(1)
            await websocket.send_json("yo")
    except ConnectionClosedOK:
        print("connection to /user/spot closed")
