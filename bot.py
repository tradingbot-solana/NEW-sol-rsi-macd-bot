import asyncio
import json
import os
import pandas as pd
import requests
from dotenv import load_dotenv

from solana.rpc.async_api import AsyncClient
from anchorpy import Provider, Wallet
from solders.keypair import Keypair
from driftpy.drift_client import DriftClient
from driftpy.drift_user import DriftUser
from driftpy.types import PositionDirection

load_dotenv()

# === CONFIG ===
PRIVATE_KEY_JSON = json.loads(os.getenv("PRIVATE_KEY_JSON"))
RPC_URL = os.getenv("RPC_URL")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
MARKET_INDEX = 0
RISK_PER_TRADE = 0.005
LEVERAGE = 5
CHECK_INTERVAL = 60

SOL_ADDRESS = "So11111111111111111111111111111111111111112"

async def get_candles():
    url = "https://public-api.birdeye.so/defi/v3/ohlcv"
    params = {"address": SOL_ADDRESS, "type": "5m", "currency": "usd", "count": 200}
    headers = {"x-api-key": BIRDEYE_API_KEY}
    
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            json_data = resp.json()
            if "data" not in json_data or "items" not in json_data["data"]:
                print(f"Birdeye response missing data/items on attempt {attempt+1}")
                await asyncio.sleep(5)
                continue
            
            data = json_data["data"]["items"]
            if not data:
                print("Birdeye returned empty items list")
                await asyncio.sleep(5)
                continue
            
            df = pd.DataFrame(data)
            rename_map = {"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            
            required_cols = ["open", "high", "low", "close", "volume"]
            if not all(col in df.columns for col in required_cols):
                print(f"Missing required columns after rename: {df.columns.tolist()}")
                await asyncio.sleep(5)
                continue
            
            return df[required_cols].astype(float)
        except Exception as e:
            print(f"Candle fetch attempt {attempt+1} failed: {e}")
            await asyncio.sleep(5)
    print("All candle fetch attempts failed — skipping cycle")
    return None

def calc_indicators(df):
    if df is None or len(df) < 50:
        print("Not enough data for indicators")
        return None

    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()

    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(9).mean()
    loss = -delta.where(delta < 0, 0).rolling(9).mean()
    rs = gain / loss
    df['rsi9'] = 100 - (100 / (1 + rs))

    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_line'] = ema12 - ema26
    df['macd_signal'] = df['macd_line'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd_line'] - df['macd_signal']

    return df

async def main():
    print("=== Bot Starting ===")

    # Load wallet
    try:
        keypair = Keypair.from_bytes(bytes(PRIVATE_KEY_JSON))
        wallet = Wallet(keypair)
        print(f"Keypair loaded | Public key: {keypair.pubkey()}")
    except Exception as e:
        print(f"Keypair error: {e}")
        return

    connection = AsyncClient(RPC_URL)
    provider = Provider(connection, wallet)

    # Drift Client
    print("Creating DriftClient...")
    try:
        drift_client = DriftClient(
            connection=connection,
            wallet=wallet,
            env="mainnet",
            perp_market_indexes=[MARKET_INDEX]
        )
        await drift_client.subscribe()
        print("DriftClient subscribed successfully")
    except Exception as e:
        print(f"DriftClient subscribe error: {e}")
        return

    # Drift User with retry on collateral
    drift_user = None
    collateral = None
    try:
        drift_user = DriftUser(drift_client, user_public_key=keypair.pubkey())
        print("DriftUser initialized")
        
        for attempt in range(5):
            try:
                collateral = await drift_user.get_total_collateral()
                if collateral is not None:
                    print(f"🚀 Bot is LIVE | Collateral: ${collateral:.2f}")
                    break
                else:
                    print(f"Collateral fetch returned None on attempt {attempt+1} — retrying in 10s")
                    await asyncio.sleep(10)
            except Exception as e:
                print(f"Collateral fetch attempt {attempt+1} error: {e}")
                await asyncio.sleep(10)
        else:
            print("Collateral still None after retries — check Drift account on app.drift.trade")
    except Exception as e:
        print(f"DriftUser init error: {e}")
        return

    in_position = False
    position_side = None

    while True:
        print("Starting new cycle...")
        try:
            df = await get_candles()
            if df is None:
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            df = calc_indicators(df)
            if df is None:
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            latest = df.iloc[-1]
            prev = df.iloc[-2]

            long_signal = (
                latest["close"] > latest["ema9"] > latest["ema21"] and
                prev["rsi9"] < 25 <= latest["rsi9"] and
                prev["macd_hist"] < 0 <= latest["macd_hist"]
            )

            short_signal = (
                latest["close"] < latest["ema9"] < latest["ema21"] and
                prev["rsi9"] > 75 >= latest["rsi9"] and
                prev["macd_hist"] > 0 >= latest["macd_hist"]
            )

            positions = await drift_user.get_user_positions()
            has_position = any(
                p.market_index == MARKET_INDEX and abs(p.base_asset_amount) > 0
                for p in positions
            )

            if not has_position:
                collateral = await drift_user.get_total_collateral()
                if collateral is None or collateral <= 0:
                    print("No collateral available — skipping cycle")
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                size_usd = collateral * LEVERAGE * RISK_PER_TRADE * 2
                size_base = int(size_usd / latest["close"] * 1e9)

                if long_signal:
                    await drift_client.open_position(PositionDirection.LONG(), size_base, MARKET_INDEX)
                    print(f"✅ LONG opened @ ${latest['close']:.2f}")
                    in_position = True
                    position_side = "LONG"

                elif short_signal:
                    await drift_client.open_position(PositionDirection.SHORT(), size_base, MARKET_INDEX)
                    print(f"✅ SHORT opened @ ${latest['close']:.2f}")
                    in_position = True
                    position_side = "SHORT"

            elif has_position and ((position_side == "LONG" and short_signal) or (position_side == "SHORT" and long_signal)):
                await drift_client.close_position(MARKET_INDEX)
                print(f"{position_side} closed")
                in_position = False
                position_side = None

            print(f"Cycle complete — waiting {CHECK_INTERVAL}s")
            await asyncio.sleep(CHECK_INTERVAL)

        except Exception as e:
            print(f"Loop error: {e}")
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
