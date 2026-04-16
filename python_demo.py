from kafka import KafkaProducer
import json

# Kết nối vào cửa ngõ Kafka đang mở ở cổng 9092 trên máy của bạn
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Đóng gói một gói tin nhỏ mang tên bạn
data = {
    "user": "Quoc Bao", 
    "status": "Success",
    "message": "Cụm Big Data đã hoạt động trơn tru!"
}

# Bắn gói tin vào một chủ đề (topic) tên là 'dack_streaming'
print("Đang gửi dữ liệu...")
producer.send('dack_streaming', data)

# Đảm bảo dữ liệu đã đi hết trước khi đóng code
producer.flush()
print("Đã bắn tin nhắn thành công vào Kafka!")