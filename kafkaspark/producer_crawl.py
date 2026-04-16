from kafka import KafkaProducer
import json
import time
import random

# 1. KẾT NỐI: Trỏ thẳng vào trạm trung chuyển Kafka đang chạy ở cổng 9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Tên của "Đường ống" mà ta sẽ ném dữ liệu vào
TOPIC_NAME = 'crawled_data'

def run_crawler():
    print("🚀 Bắt đầu chạy Crawler và đẩy data vào Kafka...")
    count = 1
    
    # Vòng lặp vô hạn để mô phỏng việc cào dữ liệu liên tục
    while True:
        # =========================================================
        # CHỖ NÀY DÀNH CHO CODE CỦA BẠN:
        # Sau này bạn nhét logic Requests / BeautifulSoup của file 
        # test_crawl.py gốc vào đây để lấy data thật.
        # =========================================================
        
        # Hiện tại: Tạo DỮ LIỆU GIẢ LẬP để test hệ thống trước
        data = {
            "id": count,
            "title": f"Bài viết tin tức số {count}",
            "category": random.choice(["Thể thao", "Công nghệ", "Kinh doanh", "Giải trí"]),
            "timestamp": time.time()
        }
        
        # 2. BẮN DỮ LIỆU: Ném gói data vừa tạo vào đường ống Kafka
        producer.send(TOPIC_NAME, value=data)
        print(f"[Đã gửi] {data}")
        
        count += 1
        # Nghỉ 2 giây trước khi cào bài tiếp theo (tránh bị block IP)
        time.sleep(2) 

if __name__ == "__main__":
    run_crawler()