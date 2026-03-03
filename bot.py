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
MARKET_INDEX = 0  # SOL-PERP
RISK_PER_TRADE = 0.005
LEVERAGE = 5      # Start low for safety
CHECK_INTERVAL = 60

SOL_ADDRESS = "So11111111111111111111111111111111111111112"

async def get_candles():
    url = "https://public-api.birdeye.so/defi/v3/ohlcv"
    params = {
        "address": SOL_ADDRESS,
        "type": "5m",
        "currency": "usd",
        "count": 200
    }
    headers = {"x-api-key": BIRDEYE_API_KEY}
    
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()["data"]["items"]
            df = pd.DataFrame(data)
            df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
            return df[["open", "high", "low", "close", "volume"]].astype(float)
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

    # Drift User
    try:
        drift_user = DriftUser(drift_client, user_public_key=keypair.pubkey())
        collateral = await drift_user.get_total_collateral()
        if collateral is None:
            print
