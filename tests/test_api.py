#!/usr/bin/env python
# -*- coding: utf-8 -*-


from unittest import TestCase

import pandas as pd

from fastapi.testclient import TestClient
from api.api import app, SYMBOLS


class RESTTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def test_read_account(self):
        """
        Ensure account endpoint returns correct keys for correct symbols.

        """

        response = self.client.get("/account")

        data = response.json()

        symbols = [row["symbol"] for row in data]
        account_keys = [
            "symbol",
            "positionAmt",
            "entryPrice",
            "markPrice",
            "unRealizedProfit",
            "liquidationPrice",
            "leverage",
            "maxNotionalValue",
            "marginType",
            "isolatedMargin",
            "isAutoAddMargin",
            "positionSide",
            "notional",
            "isolatedWallet",
            "updateTime",
            "marginPositionAmt",
            "marginEntryPrice",
            "marginNotional",
            "margin",
        ]

        self.assertEqual(response.status_code, 200)
        self.assertEqual(sorted(symbols), sorted(SYMBOLS))
        self.assertEqual(sorted(account_keys), sorted(data[0].keys()))

    def test_read_wallet(self):
        """
        Ensure wallet endpoint returns correct keys for each wallet asset.
        """

        response = self.client.get("/wallet")

        data = response.json()

        assets = sorted([row["asset"] for row in data])
        wallet_keys = [
            "asset",
            "futuresPositionAmt",
            "marginPositionAmt",
            "totalPositionAmt",
        ]

        self.assertEqual(response.status_code, 200)
        self.assertEqual(assets, ["BNB", "BUSD", "USDT"])
        self.assertEqual(sorted(wallet_keys), sorted(data[0].keys()))

    def test_read_trades(self):
        """
        Ensure trades endpoint returns trades with correct keys.
        """

        response = self.client.get("/trades")

        data = response.json()

        float_cols = ["realizedPnlFuture", "realizedPnlMargin", "realizedPnlTotal"]

        df = pd.DataFrame(data).astype({c: float for c in float_cols})

        keys = [
            "id",
            "symbol",
            "time",
            "orderIdFuture",
            "sideFuture",
            "qtyFuture",
            "priceFuture",
            "quoteQtyFuture",
            "commissionFuture",
            "realizedPnlFuture",
            "orderIdMargin",
            "sideMargin",
            "qtyMargin",
            "priceMargin",
            "quoteQtyMargin",
            "commissionMargin",
            "realizedPnlMargin",
            "commissionTotal",
            "realizedPnlTotal",
        ]

        self.assertEqual(response.status_code, 200)
        self.assertEqual(sorted(keys), sorted(data[0].keys()))
        self.assertAlmostEqual(
            df.realizedPnlFuture.sum() + df.realizedPnlMargin.sum(),
            df.realizedPnlTotal.sum(),
            6,
        )

    def test_klines(self):
        """
        Ensure klines endpoint returns trades with correct keys and that
        spread is calculated correctly.
        """

        keys = ["time", "open", "high", "low", "close", "volume"]

        dfs = {}
        for m in ["spot", "futures", "spread"]:
            response = self.client.get(f"/klines/{m}/BTCUSDT")
            dfs[m] = pd.DataFrame(response.json(), dtype=float)
            with self.subTest(market=m):
                self.assertEqual(sorted(keys), sorted(dfs[m]))
                self.assertEqual(response.status_code, 200)

        self.assertAlmostEqual(
            (dfs["futures"]["close"] / dfs["spot"]["close"] - 1).sum(),
            dfs["spread"]["close"].sum(),
            2,
        )
