"""
Module: Vertex AI Real-time ML Predictor
Mô tả:
 - Bắt Message theo dòng chảy (Streaming) trên kênh Google Cloud Pub/Sub
 - Sử dụng Endpoint mở của Google Vertex AI thực thi việc Inference (Suy đoán Mô hình).
 - Đưa ra dự báo dựa trên mô hình Deep Learning Real-time với độ trễ < 50ms (Được thiết lập giả định).
"""

import os
import json
import sys
import logging
from google.cloud import pubsub_v1
from google.cloud import aiplatform

# 1. Cấu hình Logging đầy đủ theo luật Rule Code
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [VERTEX-AI-STREAM] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = ".env") -> None:
    """Nạp file .env có bọc try-catch không sợ bị sập hệ thống."""
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line: 
                        continue
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())
            logger.info("Nạp môi trường .env thành công.")
        else:
            logger.warning(f"Không tìm được đoạn cấu hình chứa Vertex Endpoint ở {path}.")
    except Exception as e:
        logger.error(f"Lỗi khi đọc file cấu hình .env: {e}")

class RealTimeVertexAIPredictor:
    def __init__(self):
        """Khởi tạo Client Pub/Sub và thiết lập kết nối nòng súng đến Vertex AI"""
        logger.info("Bắt đầu nạp Endpoint AI của Google Cloud Vertex...")
        try:
            self.project_id = os.getenv("GCP_PROJECT_ID")
            self.location = os.getenv("GCP_REGION", "us-central1") # Mặc định Data Center Mỹ
            self.subscription_id = os.getenv("GCP_PUBSUB_SUBSCRIPTION", "crypto_stream_hybrid-sub")
            self.endpoint_id = os.getenv("VERTEX_AI_ENDPOINT_ID", "1234567890123456789") 
            
            # Check bắt buộc thông số Dự án
            if not self.project_id:
                raise ValueError("Không tìm được thông tin Base GCP_PROJECT_ID")

            # Khởi tạo súng Vertex AI Client
            aiplatform.init(project=self.project_id, location=self.location)
            
        except ValueError as ve:
            logger.error(f"Cấu hình dự án bị lệch: {ve}")
            raise
        except Exception as e:
            logger.error(f"Khởi động môi trường Vertex bị rớt: {e}")
            raise

        try:
            # Rọi thằng vào cái Model Inference đã lưu trữ
            self.endpoint = aiplatform.Endpoint(self.endpoint_id)
            logger.info("Tuyến truyền dữ liệu Vertex AI đã SẴN SÀNG.")
        except Exception as e:
            logger.warning(f"Vertex AI Endpoint không khả dụng hoặc chưa Train Model. Chuyển sang chức năng Logic Phụ Cấp Demo: {e}")
            self.endpoint = None

        try:
            # Khởi tạo Cổng nghe Pub/Sub
            self.subscriber = pubsub_v1.SubscriberClient()
            self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)
        except Exception as e:
            logger.error(f"Lỗi nảy sinh cấu hình Google Cloud Pub/Sub: {e}")
            raise

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        """Hàm Auto Hook - Bị bật tung khi có Message rơi vô Pub/Sub Subscription"""
        try:
            # Giải nén lớp vỏ Payload Unicode UTF-8
            data = json.loads(message.data.decode("utf-8"))
            symbol = data.get("symbol")
            price = data.get("price")
            volume = data.get("volume")
            
            # Đóng gói làm mồi Tensor đút vào thư viện Deep Learning Mạng Nơron
            instances = [[price, volume]]
            
            prediction_result = "N/A"
            if self.endpoint:
                # Bắn đạn: Gọi API Inference thời gian thực (Tốc độ kinh hoàng 50-100ms)
                response = self.endpoint.predict(instances=instances)
                prediction_result = response.predictions[0]
            else:
                # Logic phân luồng phụ trợ (Mock Prediction) nêý Vertex AI bị tắt Endpoint (Tránh tốn $. Tiền Demo)
                prediction_result = "⚠️ Cảnh báo Rủi ro Kéo Râu Bơm Đểu" if price > 50000 and volume > 10 else "🟢 Lệnh Giao Dịch An Toàn Khớp Lệnh."

            # Ép log cực đậm để xuất màn hình terminal Console.
            logger.info(f"[Đã dự báo Realtime Output] MÃ {symbol} | Mức Giá Hiện Tại: {price}$ -> Quyết định Vertex AI: {prediction_result}")
            
            # Cực kỳ quan trọng: Acknowledge (Ấn nút báo nhận xong) để Pubsub xóa Message khỏi hàng đợi
            message.ack()

        except KeyError as ke:
            logger.error(f"Thành phần key bị sai lệch trong mảng Json: {ke}")
            message.nack() # Lệnh nack: Fail và vứt trở lại ngách hàng đợi
        except Exception as e:
            logger.error(f"Lệnh ML Inference gãy nhũn vì sự cố: {e}", exc_info=True)
            message.nack() 

    def start_streaming(self):
        """Mở vòng lặp vĩnh viễn (Puller)"""
        logger.info(f"Đang bủa lưới Streaming tại hố Sub {self.subscription_path}. Dữ liệu văng ra phát xử lý liền...")
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        try:
            streaming_pull_future.result() # Treo Thread chờ Result dài hạn
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            logger.info("Chủ thể quản trị viên đóng hệ thống. (Shutdown triggered)")
        except Exception as e:
            logger.critical(f"Lệnh Streaming bị Crash chấn thương: {e}", exc_info=True)

def main():
    try:
        load_dotenv_file()
        
        # Ngưỡng bảo vệ an ninh Auth
        if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            logger.error("🛑 Dừng cỗ xe: Không thấy Google Credentials để Auth!")
            return
            
        predictor = RealTimeVertexAIPredictor()
        predictor.start_streaming()
    except Exception as e:
        logger.critical(f"Hệ thống lõi Main Program tử trận trước khi lên nòng: {e}")

if __name__ == "__main__":
    main()
