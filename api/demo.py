import asyncio
from enum import Enum
from functools import partial
import json
import os
from re import L

from binance import AsyncClient, BinanceSocketManager, enums
from fastapi import FastAPI
from fastapi import Request
from fastapi import WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd


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


@app.get("/")
def read_root(request: Request):
    return templates.TemplateResponse("index.htm", {"request": request})


@app.get("/klines/{market}/{symbol}")
async def get_kline_history(market: marketName, symbol: str):
    client = await async_client()

    methods = {
        marketName.futures: client.futures_klines,
        marketName.spot: client.get_klines,
    }

    if market == marketName.futures:
        res = await client.futures_klines(
            symbol=symbol, interval=client.KLINE_INTERVAL_1MINUTE
        )

    res = await methods[market](symbol=symbol, interval=client.KLINE_INTERVAL_1MINUTE)

    processed_klines = [
        {key: value for key, value in zip(candle_keys, kline)} for kline in res
    ]

    for k in processed_klines:
        k["time"] = k["time"] / 1000

    return processed_klines


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

                if kline_spots[0] and kline_futures:

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


@app.websocket("/market-stream/futures")
async def get_market_stream_futures(websocket: WebSocket):
    await websocket.accept()

    client = await async_client()
    bm = BinanceSocketManager(client)

    async with bm._get_futures_socket(
        path=f"!markPrice@arr", futures_type=enums.FuturesType.USD_M
    ) as stream:
        while True:
            res = await stream.recv()
            await websocket.send_json(res)
