import asyncio
import json
import os
import time
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
STOP_LOSS_PCT = 0.05  # 5% fixed SL
TRAILING_OFFSET = 0.05  # 5% trailing from high/low water mark

SOL_ADDRESS = "So11111111111111111111111111111111111111112"

# Validate env vars
if not all([PRIVATE_KEY_JSON, RPC_URL, BIRDEYE_API_KEY]):
    raise ValueError("Missing required env vars: PRIVATE_KEY_JSON, RPC_URL, or BIRDEYE_API_KEY")

async def get_current_price():
    """Fetch latest SOL/USD price from Birdeye."""
    url = "https://public-api.birdeye.so/defi/price"
    params = {
        "address": SOL_ADDRESS,
        "include_liquidity": "false"
    }
    headers = {
        "x-api-key": BIRDEYE_API_KEY,
        "x-chain": "solana",
        "accept": "application/json"
    }
    
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success") and "data" in data and "value" in data["data"]:
            return float(data["data"]["value"])
        else:
            print(f"Birdeye price response invalid: {data}")
            return None
    except Exception as e:
        print(f"Current price fetch failed: {e}")
        return None

async def get_candles():
    now = int(time.time())
    time_to = now
    time_from = now - (200 * 3600)  # Approx 200 1h candles
    
    url = "https://public-api.birdeye.so/defi/v3/ohlcv"
    params = {
        "address": SOL_ADDRESS,
        "type": "1h",
        "currency": "usd",
        "time_from": time_from,
        "time_to": time_to,
        "ui_amount_mode": "raw"
    }
    headers = {
        "x-api-key": BIRDEYE_API_KEY,
        "x-chain": "solana",
        "accept": "application/json"
    }
    
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 429:
                print("Rate limit hit — backing off")
                await asyncio.sleep(60 * (attempt + 1))
                continue
            resp.raise_for_status()
            json_data = resp.json()
            
            if not json_data.get("success", False):
                print(f"Birdeye success=false: {json_data}")
                await asyncio.sleep(5)
                continue
            
            if "data" not in json_data or "items" not in json_data["data"]:
                print(f"Missing 'data/items' – full: {json_data}")
                await asyncio.sleep(5)
                continue
            
            data = json_data["data"]["items"]
            if not data:
                print(f"Empty items list on attempt {attempt+1}")
                await asyncio.sleep(5)
                continue
            
            df = pd.DataFrame(data)
            rename_map = {"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            
            required = ["open", "high", "low", "close", "volume"]
            missing = [col for col in required if col not in df.columns]
            if missing:
                print(f"Missing columns: {missing} | Available: {df.columns.tolist()}")
                await asyncio.sleep(5)
                continue
            
            return df[required].astype(float)
        
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error {resp.status_code}: {resp.text}")
            await asyncio.sleep(5)
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

    # Full Wilder RSI (14-period standard)
    period = 14
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    
    # Initial averages
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    
    # Smoothed RSI (Wilder method)
    for i in range(period, len(df)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period
    
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))

    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_line'] = ema12 - ema26
    df['macd_signal'] = df['macd_line'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd_line'] - df['macd_signal']

    return df

async def main():
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("=== Bot Starting ===")

    # Load wallet
    try:
        keypair = Keypair.from_bytes(bytes(PRIVATE_KEY_JSON))
        wallet = Wallet(keypair)
        logger.info(f"Keypair loaded | Public key: {keypair.pubkey()}")
    except Exception as e:
        logger.error(f"Keypair error: {e}")
        return

    connection = AsyncClient(RPC_URL)
    provider = Provider(connection, wallet)

    # Drift Client
    logger.info("Creating DriftClient...")
    try:
        drift_client = DriftClient(
            connection=connection,
            wallet=wallet,
            env="mainnet",
            perp_market_indexes=[MARKET_INDEX]
        )
        await drift_client.subscribe()
        logger.info("DriftClient subscribed successfully")
    except Exception as e:
        logger.error(f"DriftClient subscribe error: {e}")
        return

    # Drift User
    drift_user = None
    try:
        drift_user = DriftUser(drift_client, user_public_key=keypair.pubkey())
        logger.info("DriftUser initialized")
        
        for attempt in range(5):
            try:
                collateral = await drift_user.get_total_collateral()
                if collateral is not None:
                    logger.info(f"🚀 Bot is LIVE | Collateral: ${collateral:.2f}")
                    break
                else:
                    logger.warning(f"Collateral None on attempt {attempt+1} — retrying in 10s")
                    await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Collateral fetch attempt {attempt+1} error: {e}")
                await asyncio.sleep(10)
        else:
            logger.error("Collateral still None after retries — check Drift account")
            return
    except Exception as e:
        logger.error(f"DriftUser init error: {e}")
        return

    # Position tracking vars (reset on close)
    entry_price = None
    initial_sl_price = None
    high_water_mark = None  # for long
    low_water_mark = None   # for short
    position_side = None

    while True:
        logger.info("Starting new cycle...")
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
                prev["rsi"] < 30 <= latest["rsi"] and
                prev["macd_hist"] < 0 <= latest["macd_hist"] and
                latest["volume"] > df["volume"].mean() * 1.2
            )

            short_signal = (
                latest["close"] < latest["ema9"] < latest["ema21"] and
                prev["rsi"] > 70 >= latest["rsi"] and
                prev["macd_hist"] > 0 >= latest["macd_hist"] and
                latest["volume"] > df["volume"].mean() * 1.2
            )

            # Get current positions from on-chain (don't rely on local vars)
            positions = await drift_user.get_user_positions()
            sol_perp_pos = next((p for p in positions if p.market_index == MARKET_INDEX), None)
            
            has_long = sol_perp_pos and sol_perp_pos.base_asset_amount > 0
            has_short = sol_perp_pos and sol_perp_pos.base_asset_amount < 0
            has_position = has_long or has_short

            current_price = await get_current_price()
            if current_price is None:
                logger.warning("Failed to fetch current price — skipping trade actions")
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            if has_position:
                # Update position_side if not set (e.g., on restart)
                if position_side is None:
                    position_side = "LONG" if has_long else "SHORT"

                # Monitor SL and trailing SL
                if position_side == "LONG":
                    high_water_mark = max(high_water_mark or current_price, current_price)
                    trailing_sl = high_water_mark * (1 - TRAILING_OFFSET)
                    if current_price <= trailing_sl or current_price <= initial_sl_price:
                        await drift_client.close_position(MARKET_INDEX)
                        logger.info(f"LONG closed on SL hit @ ${current_price:.2f} (trailing: {trailing_sl:.2f}, initial: {initial_sl_price:.2f})")
                        entry_price = None
                        initial_sl_price = None
                        high_water_mark = None
                        position_side = None

                elif position_side == "SHORT":
                    low_water_mark = min(low_water_mark or current_price, current_price)
                    trailing_sl = low_water_mark * (1 + TRAILING_OFFSET)
                    if current_price >= trailing_sl or current_price >= initial_sl_price:
                        await drift_client.close_position(MARKET_INDEX)
                        logger.info(f"SHORT closed on SL hit @ ${current_price:.2f} (trailing: {trailing_sl:.2f}, initial: {initial_sl_price:.2f})")
                        entry_price = None
                        initial_sl_price = None
                        low_water_mark = None
                        position_side = None

                # Also check for reversal signals to close
                if (position_side == "LONG" and short_signal) or (position_side == "SHORT" and long_signal):
                    await drift_client.close_position(MARKET_INDEX)
                    logger.info(f"{position_side} closed on reversal signal")
                    entry_price = None
                    initial_sl_price = None
                    high_water_mark = None
                    low_water_mark = None
                    position_side = None

            else:
                collateral = await drift_user.get_total_collateral()
                if collateral is None or collateral <= 0:
                    logger.warning("No collateral available — skipping cycle")
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                # Position size calc (unchanged, as per request)
                size_usd = collateral * LEVERAGE * RISK_PER_TRADE * 2
                size_base = int(size_usd / latest["close"] * 1e9)

                if long_signal:
                    await drift_client.open_position(PositionDirection.LONG(), size_base, MARKET_INDEX)
                    logger.info(f"✅ LONG opened @ ${current_price:.2f}")
                    entry_price = current_price
                    initial_sl_price = entry_price * (1 - STOP_LOSS_PCT)
                    high_water_mark = entry_price
                    position_side = "LONG"

                elif short_signal:
                    await drift_client.open_position(PositionDirection.SHORT(), size_base, MARKET_INDEX)
                    logger.info(f"✅ SHORT opened @ ${current_price:.2f}")
                    entry_price = current_price
                    initial_sl_price = entry_price * (1 + STOP_LOSS_PCT)
                    low_water_mark = entry_price
                    position_side = "SHORT"

            # Health check
            health = await drift_user.get_health_factor()
            if health < 1.2:
                logger.warning(f"Account health low ({health:.2f}) — pausing 5min")
                await asyncio.sleep(300)

            logger.info(f"Cycle complete — waiting {CHECK_INTERVAL}s")
            await asyncio.sleep(CHECK_INTERVAL)

        except Exception as e:
            logger.error(f"Loop error: {e}")
            await asyncio.sleep(30)  # Short backoff, increase exponentially if needed

if __name__ == "__main__":
    asyncio.run(main())
