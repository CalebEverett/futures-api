import asyncio
import json
import os

from fastapi import FastAPI
from fastapi import Request
from fastapi import WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
import websockets
from fastapi.staticfiles import StaticFiles
from binance import Client
from binance import AsyncClient, BinanceSocketManager, enums

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

client = Client(
    api_key=os.getenv("BINANCE_API_KEY"), api_secret=os.getenv("BINANCE_API_SECRET")
)


@app.get("/")
def read_root(request: Request):
    return templates.TemplateResponse("index.htm", {"request": request})


@app.get("/klines/{market}/{symbol}")
async def get_klines(market: str, symbol: str):

    if market == "futures":
        res = client.futures_klines(
            symbol=symbol, interval=client.KLINE_INTERVAL_1MINUTE
        )
    else:
        res = client.get_klines(symbol=symbol, interval=client.KLINE_INTERVAL_1MINUTE)

    keys = ["time", "open", "high", "low", "close"]

    processed_klines = [
        {key: value for key, value in zip(keys, kline)} for kline in res
    ]

    for k in processed_klines:
        k["time"] = k["time"] / 1000

    return processed_klines


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client = await AsyncClient.create(
        api_key=os.getenv("BINANCE_API_KEY"), api_secret=os.getenv("BINANCE_API_SECRET")
    )

    bm = BinanceSocketManager(client)
    async with bm._get_futures_socket(
        path=f"btcusdt@kline_1m", futures_type=enums.FuturesType.USD_M
    ) as stream:
        while True:
            res = await stream.recv()
            print(res["data"])
            await websocket.send_json(res["data"])


# @app.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     while True:
#         await websocket.send_text("fongulo")
