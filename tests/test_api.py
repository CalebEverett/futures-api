#!/usr/bin/env python
# -*- coding: utf-8 -*-


from unittest import TestCase
from fastapi.testclient import TestClient
from api.api import app, SYMBOLS

client = TestClient(app)


class EndPointTests(TestCase):
    def test_read_account(self):
        """
        Ensure account endpoint returns correct keys for correct symbols.
        """

        response = client.get("/account")

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
        self.assertEquals(sorted(symbols), sorted(SYMBOLS))
        self.assertEquals(sorted(account_keys), sorted(data[0].keys()))

    def test_read_wallet(self):
        """
        Ensure wallet endpoint returns correct keys for each wallet asset.
        """

        response = client.get("/wallet")

        data = response.json()

        assets = sorted([row["asset"] for row in data])
        wallet_keys = [
            "asset",
            "futuresPositionAmt",
            "marginPositionAmt",
            "totalPositionAmt",
        ]

        self.assertEqual(response.status_code, 200)
        self.assertEquals(assets, ["BNB", "BUSD", "USDT"])
        self.assertEquals(sorted(wallet_keys), sorted(data[0].keys()))
