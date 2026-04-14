"""
Tiện ích Kafka Consumer (Chỉ dùng để kiểm thử)
Kịch bản này hỗ trợ việc kiểm tra và theo dõi các điểm dữ liệu trực tiếp 
từ nhánh (topic) Kafka một cách thủ công qua Terminal.
"""

import json
import logging
import os
from typing import Dict, Any
from kafka import KafkaConsumer

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = ".env") -> None:
    """Nạp các biến môi trường từ tập tin .env cục bộ."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())

def main() -> None:
    # Khởi tạo cấu hình môi trường
    load_dotenv_file()

    # Thuộc tính cụm Kafka
    kafka_broker: str = os.getenv("KAFKA_BROKER", "localhost:9094")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "crypto_trades")

    logger.info(f"Đang chờ dòng dữ liệu từ Kafka Broker tại {kafka_broker}...")
    
    try:
        # Khởi tạo tiến trình Kafka consumer
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=[kafka_broker],
            auto_offset_reset='earliest',  # Luôn luôn đọc từ những dữ liệu đầu tiên nếu chưa có lịch sử đọc
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Liên tục giám sát tin nhắn mới
        for message in consumer:
            data: Dict[str, Any] = message.value
            
            # Xác thực cấu trúc dữ liệu gửi tới (Schema Format)
            is_valid_payload = isinstance(data, dict) and all(key in data for key in ['time', 'symbol', 'price', 'volume'])
            
            if is_valid_payload:
                logger.info(f"📦 [XUẤT TỪ KAFKA] -> Thời gian: {data['time']}, Đồng coin: {data['symbol']}, "
                            f"Giá: {data['price']}, Khối lượng: {data['volume']}")
            else:
                logger.warning(f"⚠️ [DỮ LIỆU KHÁC/CŨ] -> {data}")
                
    except Exception as e:
        logger.error(f"Đã xảy ra lỗi trong quá trình chạy Consumer: {e}")

if __name__ == "__main__":
    main()
