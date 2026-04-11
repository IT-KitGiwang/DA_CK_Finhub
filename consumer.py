from kafka import KafkaConsumer
import json
import os


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

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_trades")

print("Đang chờ dữ liệu từ Kafka...")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest', # Đọc từ đầu nếu chưa đọc
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    data = message.value
    print(f"📦 [XUẤT TỪ KAFKA] -> Thời gian: {data['time']}, Đồng coin: {data['symbol']}, Giá: {data['price']}")
