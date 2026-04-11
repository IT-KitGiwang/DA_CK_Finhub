import websocket
import json
import os
from datetime import datetime
from kafka import KafkaProducer

# Mật khẩu API giả lập từ Finnhub
API_KEY = "d6g5sv1r01qt4931ljbgd6g5sv1r01qt4931ljc0"

# Cấu hình Kafka Kafka (Chạy trên cổng 9094 của localhost đẩy dữ liệu vào Cluster Docker)
KAFKA_BROKER = 'localhost:9094'
KAFKA_TOPIC = 'crypto_trades'

# Khởi tạo Kafka Producer (Chuyển đổi dữ liệu Python Dict sang chuỗi JSON và mã hóa Bytes)
print(f"Connecting to Kafka Broker: {KAFKA_BROKER}...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Danh sách symbol cần lấy
SYMBOLS = [
    "BINANCE:BTCUSDT",
    "BINANCE:ETHUSDT",
    "BINANCE:BNBUSDT"
]

def on_message(ws, message):
    data = json.loads(message)

    if data.get("type") == "trade":
        for trade in data["data"]:
            symbol = trade.get("s")
            price = trade.get("p")
            volume = trade.get("v")
            timestamp = trade.get("t")

            # Chuyển đổi timestamp
            time_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

            # Tạo Payload (gói dữ liệu chuẩn bị gửi đi)
            payload = {
                "time": time_str,
                "symbol": symbol,
                "price": price,
                "volume": volume
            }

            # 👉 THAY VÌ GHI RA FILE CSV LOCAL -> ĐẨY THẲNG VÀO KAFKA CHỜ SPARK XỬ LÝ!
            producer.send(KAFKA_TOPIC, value=payload)
            
            print(f"[KAFKA SENT] - {time_str} | {symbol} | Price: {price} | Vol: {volume}")


def on_error(ws, error):
    print("ERROR:", error)

def on_close(ws):
    print("CLOSED")
    producer.close()

def on_open(ws):
    print("Connected to WebSocket!")

    # Gửi lệnh Subscribe cho nhiều symbol
    for symbol in SYMBOLS:
        ws.send(json.dumps({
            "type": "subscribe",
            "symbol": symbol
        }))
        print(f"Subscribed: {symbol}")

if __name__ == "__main__":
    # Kết nối tới API
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={API_KEY}",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    # Chạy vòng lặp vô tận
    try:
        ws.run_forever()
    except KeyboardInterrupt:
        producer.close()
        print("\nProducer stopped.")
