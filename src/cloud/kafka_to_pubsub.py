"""
Module: Hybrid Cloud Pub/Sub Bridge
Mô tả: 
 - Rẽ nhánh lấy Message trực tiếp từ Kafka Broker ở trạm on-premise
 - Bắn lập tức lên Google Cloud Pub/Sub thông qua API của Google.
 - Cho phép mảng Serverless (Dataflow/Cloud Functions) tận dụng dòng dữ liệu Realtime.
"""

import os
import json
import logging
import sys
import time
from kafka import KafkaConsumer
from google.cloud import pubsub_v1

# 1. Cấu hình Logging đầy đủ theo luật Rule Code
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [PUBSUB-BRIDGE] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = ".env") -> None:
    """
    Hàm đọc cấu hình từ file .env với try-catch không để crash chương trình.
    """
    try:
        if not os.path.exists(path):
            logger.warning(f"Không tìm được file env ở đường dẫn {path}.")
            return
        
        with open(path, "r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line: 
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())
        logger.info("Nạp môi trường .env thành công.")
    except Exception as e:
        logger.error(f"Lỗi khi nạp file .env: {e}")

class KafkaToPubSubBridge:
    def __init__(self):
        """Khởi tạo các thành phần kết nối Kafka và Google Pub/Sub"""
        logger.info("Tiến hành nặn thông tin kết nối từ cấu hình môi trường...")
        try:
            # Thu nạp thông tin Kafka
            self.kafka_broker = os.getenv("KAFKA_BROKER")
            self.kafka_topic = os.getenv("KAFKA_TOPIC")

            # Thu nạp thông tin Google Pub/Sub
            self.project_id = os.getenv("GCP_PROJECT_ID")
            self.pubsub_topic_id = os.getenv("GCP_PUBSUB_TOPIC")
            
            # Khởi tạo Kafka Consumer để nghe dữ liệu
            logger.info("Đang xây dựng cầu nối Kafka Consumer...")
            self.consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=[self.kafka_broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='gcp-pubsub-bridge-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            # Khởi tạo Pub/Sub Publisher
            logger.info("Đang xây dựng cầu nối Pub/Sub Publisher...")
            self.publisher = pubsub_v1.PublisherClient()
            self.topic_path = self.publisher.topic_path(self.project_id, self.pubsub_topic_id)
            
            logger.info(f"Kết nối thành công. Pipeline Bridge: [{self.kafka_topic}] (Local) ➔ [{self.topic_path}] (GCP)")
            
        except TypeError as te:
            logger.error(f"Lỗi thiếu biến cấu hình hệ thống (NoneType error): {te}")
            raise
        except Exception as e:
            logger.error(f"Lỗi không xác định khi khởi tạo Bridge kết nối: {e}")
            raise

    def run(self):
        """Khởi chạy vòng lặp bắt gói tin Kafka và bắn sang Pub/Sub"""
        logger.info("🔥 Bắt đầu vòng lặp chuyển tiếp dữ liệu lên GCP Pub/Sub. Hệ thống đang chờ tín hiệu...")
        try:
            for message in self.consumer:
                try:
                    # Lấy cục dữ liệu từ message
                    data = message.value
                    symbol = data.get("symbol", "UNKNOWN")
                    
                    # Chuyển payload sang dạng bytes để có thể bắn qua HTTP request của Google
                    data_bytes = json.dumps(data).encode("utf-8")
                    
                    # Bắn bất đồng bộ sang Cloud Pub/sub và nạp thêm Attributes
                    future = self.publisher.publish(
                        self.topic_path, 
                        data_bytes, 
                        symbol=symbol,
                        origin="local-docker-bridge"
                    )
                    
                    # Trả chờ lệnh kết thúc (Result) lấy message_id thành công
                    message_id = future.result()
                    logger.info(f"[THÀNH CÔNG] - Đã bắn gói tín hiệu cặp giao dịch {symbol} lên Pub/Sub. (ID: {message_id})")
                    
                except Exception as inner_e:
                    # Bắt lỗi vòng lặp chi tiết không cho crack server
                    logger.error(f"Cảnh báo: Bắn thất bại gói tín hiệu {symbol}: {inner_e}")
                    continue

        except KeyboardInterrupt:
            logger.info("Người dùng ngừng cầu nối (Ctrl+C). Bridge đã tắt an toàn.")
        except Exception as e:
            logger.critical(f"Lỗi nghiêm trọng phá vỡ vòng lặp luồng chính: {e}", exc_info=True)
        finally:
            logger.info("Tiến hành đóng sập các ổ kết nối (Closing connectors).")
            self.consumer.close()

def main():
    try:
        load_dotenv_file()
        
        # Bắt buộc check Project ID GCP tránh Null exception
        if not os.getenv("GCP_PROJECT_ID"):
            logger.error("🛑 Dừng khẩn cấp: Không đọc được biến GCP_PROJECT_ID. Cầu nối không thể tìm đường lên Cloud!")
            return
            
        bridge = KafkaToPubSubBridge()
        bridge.run()
    except Exception as e:
        logger.critical(f"Quá trình chạy file bridge bị gãy sụp: {e}")

if __name__ == "__main__":
    main()
