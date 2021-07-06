[![build status](https://github.com/CalebEverett/futures-api/actions/workflows/build.yml/badge.svg)](https://github.com/CalebEverett/futures-api/actions/workflows/build.yml)

# Futures Strategy API

## Overview

This is an api with rest and websocket endpoints that supports a prototype application to execute and monitor a spread trading strategy involving perpetual futures contracts. The basic strategy is to establish a short position in the perpetual futures contract to enable capture of funding rate payments every eight hours. A corresponding long position in the underlying asset is established at the same time to avoid taking any directional risk.

This api provides data on wallet balances, realized profit and loss, open positions, historical prices and completed trades, including real time updates via websocket. 

## Install Dependencies

Create a virtual environment and install dependencies with

    pipenv install

## Create .env file with API key and secret

* This api is designed to work with the Binance.com API.
* It requires both a margin account and futures account on binance.com.
* Instructions on how to create an API key can be found [here](https://www.binance.com/en/support/faq/360002502072).
* The key and secret should appear in a .env file as

```
BINANCE_API_KEY=<your key>
BINANCE_API_SECRET=<your secret>
```

## Fund binance.com wallets

* The api requires USDT balances in both the margin and futures accounts
* It also requires a small balance of Binance Coin (BNB) in the margin account to cover transaction costs. This is to avoid having commissions deducted from the traded asset, which otherwise results in a slight mismatches in the value of the short and long positions and odd lot sizes, complicating trade execution and leaving small amounts of orphaned assets. Doing so also has the added benefit of reducing margin account commissions from 0.1% to 0.075%.

## Start the server

    pipenv run uvicorn api.api:app --reload


## Documentation

OpenApi documentation of the REST endpoints is available at the `/docs` endpoint.


## Implementation Details

The api is implemented using [FastAPI](https://fastapi.tiangolo.com/) with routes served asynchronously with [asyncio](https://docs.python.org/3/library/asyncio.html). FastAPI also supports [websocket endpoints via Starlette](https://www.starlette.io/websockets/), which are used to stream real time updates to most application components. The api uses [python-binance](https://github.com/sammchardy/python-binance), which also has an [asynchronous client](https://sammchardy.github.io/async-binance-basics/) to fetch data from the binance.com api. Some endpoints are close to being pass throughs of the python-binance (and in turn, binance.com api) endpoints, but in many cases additional coding was required to either link the individual futures and margin positions together or to combine data from multiple endpoints as required by application components.


