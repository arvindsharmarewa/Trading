import requests
import pandas as pd
import numpy as np
import time
import hmac
import hashlib
import json
from dotenv import load_dotenv
import os
from datetime import datetime
from delta_rest_client import DeltaRestClient
from enum import Enum

load_dotenv()
# Constants for trading
BASE_URL = "https://api.india.delta.exchange"
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
SYMBOL = 'BTC/USDT'
PRODUCT_ID = 27
TIMEFRAME = "15m"
RISK_PER_TRADE = 0.005
today_date = datetime.now().strftime("%Y-%m-%d")
filename = f"signals_output_{today_date}.csv"

delta_client = DeltaRestClient(
  base_url= BASE_URL,
  api_key= API_KEY,
  api_secret= API_SECRET,
)

class OrderType(Enum):
    MARKET = "market_order"
    LIMIT = "limit_order"

# Mock balance for demonstration
balance = 1000  # Starting balance in USD
current_position = {"side": None, "size": 0, "entry_price": 0, "target_price": 0, "stop_loss": 0, "partial_booked": False}

def identify_key_levels(df, window=25):
    df["is_support"] = df["low"] == df["low"].rolling(window=window).min()
    df["is_resistance"] = df["high"] == df["high"].rolling(window=window).max()
    return df


# Fetch candlestick data
def fetch_candlestick_data(symbol, interval="15m", limit=100):
    url = f"{BASE_URL}/v2/ohlc/{symbol}/candles"
    params = {"resolution": interval, "limit": limit}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json().get("result")
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        return df
    else:
        print(f"Failed to fetch data: {response.json()}")
        return None

def fetch_candlestick_data(symbol, timeframe="15m", limit=100):
    try:
        # data = client.get_historical_candles(product_id=symbol, resolution=interval, limit=limit)
        # df = pd.DataFrame(data)
        # df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        # return df
        # ccxt
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        # Create a DataFrame
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        print(f"Error fetching candlestick data: {e}")
        return None

# Generate API signature
def generate_signature(api_secret, request_path, body=""):
    payload = request_path + body
    signature = hmac.new(api_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return signature


# Place an order
def place_order(symbol, product_id, side, quantity, price=None, order_type=OrderType.MARKET):
    try:
        order = delta_client.place_order(
            product_id = product_id,
            size = 2,
            side = side,
            order_type= order_type,
        )
        print(f"Order placed successfully: {order}")
        return order
    except Exception as e:
        print(f"Error placing order: {e}")
        return None


# Calculate trade size
def calculate_trade_size(balance, entry_price, stop_loss):
    risk_amount = balance * RISK_PER_TRADE
    trade_size = risk_amount / abs(entry_price - stop_loss)
    return trade_size


# Detect signals
def detect_signals(df, key_levels, proximity=0.005):
    df["signal"] = "None"

    for i in range(2, len(df) - 2):
        # Fake breakout detection
        if (
            df.iloc[i]["high"] > key_levels["resistance"][-1] * (1 + proximity)  # Breaks above resistance
            and df.iloc[i]["close"] < key_levels["resistance"][-1]  # Closes back below resistance
        ):
            df.at[i, "signal"] = "SellFakeBreakout"

        elif (
            df.iloc[i]["low"] < key_levels["support"][-1] * (1 - proximity)  # Breaks below support
            and df.iloc[i]["close"] > key_levels["support"][-1]  # Closes back above support
        ):
            df.at[i, "signal"] = "BuyFakeBreakout"

        # M-pattern above resistance
        elif (
            df.iloc[i - 1]["high"] > df.iloc[i - 2]["high"]
            and df.iloc[i + 1]["high"] < df.iloc[i]["high"]
            and abs(df.iloc[i]["high"] - key_levels["resistance"][-1]) / key_levels["resistance"][-1] <= proximity
        ):
            df.at[i, "signal"] = "SellMPattern"

        # W-pattern below support
        elif (
            df.iloc[i - 1]["low"] < df.iloc[i - 2]["low"]
            and df.iloc[i + 1]["low"] > df.iloc[i]["low"]
            and abs(df.iloc[i]["low"] - key_levels["support"][-1]) / key_levels["support"][-1] <= proximity
        ):
            df.at[i, "signal"] = "BuyWPattern"

    df.to_csv("signals_output.csv", index=False)
    # print the last row of the DataFrame
    print(df.tail(1))
    return df


# Handle trades
def handle_trade(signal, last_close, last_swing, RISK_PER_TRADE=0.005):
    global current_position

    if signal in ["SellFakeBreakout", "SellMPattern"]:
        new_side = "sell"
    elif signal in ["BuyFakeBreakout", "BuyWPattern"]:
        new_side = "buy"
    else:
        new_side = None

    if new_side:
        # Close opposite position if any
        if current_position["side"] and current_position["side"] != new_side:
            place_order(SYMBOL, PRODUCT_ID, "close", current_position["size"])
            current_position = {"side": None, "size": 0, "entry_price": 0, "target_price": 0, "partial_booked": False}

        # Calculate trade parameters
        entry_price = last_close
        stop_loss = entry_price * (1 - RISK_PER_TRADE) if new_side == "sell" else entry_price * (1 + RISK_PER_TRADE)
        trade_size = calculate_trade_size(balance, entry_price, stop_loss)

        first_target = entry_price - (entry_price - stop_loss) if new_side == "sell" else entry_price + (entry_price - stop_loss)
        second_target = last_swing

        # Place trade
        place_order(SYMBOL, PRODUCT_ID, new_side, trade_size)

        current_position = {
            "side": new_side,
            "size": trade_size,
            "entry_price": entry_price,
            "target_price": first_target,
            "stop_loss": stop_loss,
            "partial_booked": False,
            "second_target": second_target,
        }
        print(f"Opened new position: {current_position}")


# Monitor positions
def monitor_position(current_price):
    global current_position

    if current_position["side"]:
        if not current_position["partial_booked"] and (
            (current_position["side"] == "sell" and current_price <= current_position["target_price"]) or
            (current_position["side"] == "buy" and current_price >= current_position["target_price"])
        ):
            place_order(SYMBOL, "close", current_position["size"] / 2)
            current_position["partial_booked"] = True

        elif current_position["partial_booked"] and (
            (current_position["side"] == "sell" and current_price <= current_position["second_target"]) or
            (current_position["side"] == "buy" and current_price >= current_position["second_target"])
        ):
            place_order(SYMBOL, "close", current_position["size"] / 2)
            current_position = {"side": None, "size": 0, "entry_price": 0, "target_price": 0, "stop_loss": 0, "partial_booked": False}

def save_signals(df, filename):
    if os.path.exists(filename):
        # Append to the file if it exists
        df.to_csv(filename, mode='a', index=False, header=False)
        print(f"Appended data to {filename}")
    else:
        # Save as a new file if it doesn't exist
        df.to_csv(filename, index=False)
        print(f"Created new file: {filename}")


# LIVE TRADING
while True:
    df = fetch_candlestick_data(SYMBOL, TIMEFRAME, limit=1)
    if df is not None:
        save_signals(df, filename)
        key_levels = identify_key_levels(df, 25)
        df = detect_signals(df, key_levels)
        for i, row in df.iterrows():
            if row["signal"] != "None":
                handle_trade(row["signal"], row["close"], key_levels["resistance"][-1] if "Sell" in row["signal"] else key_levels["support"][-1])
            monitor_position(row["close"])
        last_close = df["close"].iloc[-1]
        monitor_position(last_close)
    time.sleep(15 * 60) 
