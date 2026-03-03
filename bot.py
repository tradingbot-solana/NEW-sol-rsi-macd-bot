import asyncio
import json
import os
import pandas as pd
import requests
from dotenv import load_dotenv

from solana.rpc.async_api import AsyncClient
from anchorpy import Provider, Wallet
from solders.keypair import Keypair
from driftpy.drift_client import DriftClient      # ← Correct import (with underscore)
from driftpy.drift_user import DriftUser
from driftpy.types import PositionDirection

load_dotenv()

# === CONFIG ===
PRIVATE_KEY_JSON = json.loads(os.getenv("PRIVATE_KEY_JSON"))
RPC_URL = os.getenv("RPC_URL")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
MARKET_INDEX = 0
RISK_PER_TRADE = 0.005
LEVERAGE = 5          # Start low for safety
CHECK_INTERVAL = 60

async def get_candles():
    url = "https://public-api.birdeye.so/defi/v3/ohlcv"
    params = {
        "address": "So11111111111111111111111111111111111111112",
        "type": "5m",
        "currency": "usd",
        "count": 200
    }
    headers = {"x-api-key": BIRDEYE_API_KEY}
    resp = requests.get(url, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()["data"]["items"]
    df = pd.DataFrame(data)
    df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
    return df[["open", "high", "low", "close", "volume"]].astype(float)

def calc_indicators(df):
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()

    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(9).mean()
    loss = -delta.where(delta < 0, 0).rolling(9).mean()
    rs = gain / loss
    df['rsi9'] = 100 - (100 / (1 + rs))

    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_hist'] = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()

    return df

async def main():
    print("=== Bot Starting ===")

    keypair = Keypair.from_bytes(bytes(PRIVATE_KEY_JSON))
    wallet = Wallet(keypair)
    print(f"Keypair loaded | Public key: {keypair.pubkey()}")

    connection = AsyncClient(RPC_URL)
    provider = Provider(connection, wallet)

    print("Creating DriftClient...")
    drift_client = DriftClient(
        connection=connection,
        wallet=wallet,
        perp_market_indexes=[MARKET_INDEX]
    )
    await drift_client.subscribe()
    print("Creating DriftClient...")
drift_client = DriftClient(
    connection=connection,
    wallet=wallet,
    env="mainnet",
    perp_market_indexes=[MARKET_INDEX]
)
await drift_client.subscribe()
print("DriftClient subscribed successfully")

# Fixed DriftUser init with explicit pubkey
drift_user = DriftUser(drift_client, user_public_key=keypair.pubkey())
collateral = await drift_user.get_total_collateral()
print(f"🚀 Bot is LIVE | Collateral: ${collateral:.2f}")

    while True:
        try:
            df = await get_candles()
            df = calc_indicators(df)
            latest = df.iloc[-1]
            prev = df.iloc[-2]

            long_signal = (latest["close"] > latest["ema9"] > latest["ema21"] and
                          prev["rsi9"] < 25 <= latest["rsi9"] and
                          prev["macd_hist"] < 0 <= latest["macd_hist"])

            short_signal = (latest["close"] < latest["ema9"] < latest["ema21"] and
                           prev["rsi9"] > 75 >= latest["rsi9"] and
                           prev["macd_hist"] > 0 >= latest["macd_hist"])

            has_position = any(
                p.market_index == MARKET_INDEX and abs(p.base_asset_amount) > 0
                for p in await drift_user.get_user_positions()
            )

            if not has_position:
                collateral = await drift_user.get_total_collateral()
                size_usd = collateral * LEVERAGE * RISK_PER_TRADE * 2
                size_base = int(size_usd / latest["close"] * 1e9)

                if long_signal:
                    await drift_client.open_position(PositionDirection.LONG(), size_base, MARKET_INDEX)
                    print(f"✅ LONG opened @ ${latest['close']:.2f}")
                elif short_signal:
                    await drift_client.open_position(PositionDirection.SHORT(), size_base, MARKET_INDEX)
                    print(f"✅ SHORT opened @ ${latest['close']:.2f}")

            await asyncio.sleep(CHECK_INTERVAL)

        except Exception as e:
            print(f"Loop error: {e}")
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
