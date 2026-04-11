from kafka import KafkaConsumer
import json

KAFKA_BROKER = 'localhost:9094'
KAFKA_TOPIC = 'crypto_trades'

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
