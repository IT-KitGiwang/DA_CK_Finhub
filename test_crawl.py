import websocket
import json
import csv
import os
from datetime import datetime

API_KEY = "d6g5sv1r01qt4931ljbgd6g5sv1r01qt4931ljc0"

# 👉 Danh sách nhiều symbol
SYMBOLS = [
    "BINANCE:BTCUSDT",
    "BINANCE:ETHUSDT",
    "BINANCE:BNBUSDT"
]

FILE_NAME = "crypto_multi.csv"

# Tạo file nếu chưa có
if not os.path.exists(FILE_NAME):
    with open(FILE_NAME, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["time", "symbol", "price", "volume"])


def on_message(ws, message):
    data = json.loads(message)

    if data.get("type") == "trade":
        for trade in data["data"]:
            symbol = trade.get("s")   # ⚠️ lấy symbol từ data (QUAN TRỌNG)
            price = trade.get("p")
            volume = trade.get("v")
            timestamp = trade.get("t")

            time_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

            # ghi vào CSV
            with open(FILE_NAME, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([time_str, symbol, price, volume])

            print(f"{time_str} | {symbol} | {price} | {volume}")


def on_error(ws, error):
    print("ERROR:", error)


def on_close(ws):
    print("CLOSED")


def on_open(ws):
    print("Connected")

    # 👉 subscribe nhiều symbol
    for symbol in SYMBOLS:
        ws.send(json.dumps({
            "type": "subscribe",
            "symbol": symbol
        }))


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={API_KEY}",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()