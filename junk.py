import asyncio
import os

from binance import AsyncClient, BinanceSocketManager, enums


spot_values = []


async def futures_kline_listener(client):
    bm = BinanceSocketManager(client)
    async with bm._get_futures_socket(
        path="btcusdt@kline_1m", futures_type=enums.FuturesType.USD_M
    ) as stream:
        while True:
            res = await stream.recv()
            print("FUTURES", res)
            print("SPOT", len(spot_values))


async def spot_kline_listener(client):
    bm = BinanceSocketManager(client)
    async with bm.kline_socket("BTCUSDT") as stream:
        while True:
            res = await stream.recv()
            spot_values.append(res)


async def main():

    client = await AsyncClient.create(
        api_key=os.getenv("BINANCE_API_KEY"), api_secret=os.getenv("BINANCE_API_SECRET")
    )

    res = await asyncio.gather(
        futures_kline_listener(client), spot_kline_listener(client)
    )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
