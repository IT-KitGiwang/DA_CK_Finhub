import csv
import json
import time
from kafka import KafkaProducer

# Kết nối Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'crypto_multi_stream'
csv_file = 'crypto_multi.csv'

print(f"Đang đọc dữ liệu từ {csv_file} và đẩy vào Kafka topic '{topic_name}'...")

try:
    with open(csv_file, mode='r', encoding='utf-8-sig') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            # Bắn từng dòng dữ liệu của các đồng coin vào Kafka
            producer.send(topic_name, row)
            print(f"[Real-time] Đang gửi: {row}")
            
            # Tạm dừng 1 giây để bảng điện tử nhảy đều đặn
            time.sleep(1)
            
except FileNotFoundError:
    print(f"Lỗi: Không tìm thấy file {csv_file}. Vui lòng copy file này vào chung thư mục với code!")
except KeyboardInterrupt:
    print("\nĐã tắt máy bơm dữ liệu.")
finally:
    producer.flush()
    producer.close()