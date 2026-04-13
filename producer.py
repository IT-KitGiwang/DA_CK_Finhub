import websocket
import json
import os
import time
from datetime import datetime
from kafka import KafkaProducer

def load_dotenv_file(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


load_dotenv_file()

API_KEY = os.getenv("FINNHUB_API_KEY", "")
if not API_KEY:
    raise ValueError("Missing FINNHUB_API_KEY in .env")

# Cấu hình Kafka Kafka (Chạy trên cổng 9094 của localhost đẩy dữ liệu vào Cluster Docker)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_trades")

# Khởi tạo Kafka Producer (Chuyển đổi dữ liệu Python Dict sang chuỗi JSON và mã hóa Bytes)
print(f"Connecting to Kafka Broker: {KAFKA_BROKER}...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Danh sách symbol cần lấy
SYMBOLS = [symbol.strip() for symbol in os.getenv("SYMBOLS", "BINANCE:BTCUSDT,BINANCE:ETHUSDT,BINANCE:BNBUSDT").split(",") if symbol.strip()]

# Cấu hình chu kỳ cào dữ liệu (giây) - Đọc từ .env, mặc định là 1.0 giây
SCRAPE_INTERVAL = float(os.getenv("SCRAPE_INTERVAL", "1.0"))

# Biến lưu trữ thời gian gửi cuối cùng cho từng đồng coin
last_sent_times = {}

def on_message(ws, message):
    data = json.loads(message)

    if data.get("type") == "trade":
        for trade in data["data"]:
            symbol = trade.get("s")
            
            # Kiểm tra thời gian: Giới hạn SCRAPE_INTERVAL giây gửi 1 lần cho mỗi symbol
            current_time = time.time()
            if symbol in last_sent_times and current_time - last_sent_times[symbol] < SCRAPE_INTERVAL:
                continue # Bỏ qua trade này nếu chưa qua SCRAPE_INTERVAL giây
            
            # Cập nhật thời gian gửi
            last_sent_times[symbol] = current_time
            
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


def on_close(ws, *args):
    print("CLOSED", args)
    producer.close()

def on_error(ws, error):
    print(f"ERROR: {error}")

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
