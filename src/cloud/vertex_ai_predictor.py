"""
Vertex AI Real-time ML Predictor
This script consumes from Google Cloud Pub/Sub and uses Google Vertex AI endpoints
to make real-time intelligent predictions on the streaming crypto data.
"""

import os
import json
import logging
from google.cloud import pubsub_v1
from google.cloud import aiplatform

logging.basicConfig(level=logging.INFO, format="%(asctime)s [VERTEX-AI-STREAM] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

class RealTimeVertexAIPredictor:
    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID", "finhub-project-id")
        self.location = os.getenv("GCP_REGION", "us-central1")
        self.subscription_id = os.getenv("GCP_PUBSUB_SUBSCRIPTION", "crypto_stream_hybrid-sub")
        self.endpoint_id = os.getenv("VERTEX_AI_ENDPOINT_ID", "1234567890123456789") # Thay bằng ID mô hình đã Deploy
        
        # Init Vertex AI Client
        aiplatform.init(project=self.project_id, location=self.location)
        try:
            self.endpoint = aiplatform.Endpoint(self.endpoint_id)
        except Exception as e:
            logger.warning(f"Vertex AI Endpoint không khả dụng (Chưa được triển khai). Chạy ở chế độ Mock: {e}")
            self.endpoint = None

        # Init Pub/Sub Subscriber
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        try:
            data = json.loads(message.data.decode("utf-8"))
            symbol = data.get("symbol")
            price = data.get("price")
            volume = data.get("volume")
            
            # Chuẩn bị dữ liệu cấp cho Mô hình Deep Learning trên Vertex AI
            instances = [[price, volume]]
            
            prediction_result = "N/A"
            if self.endpoint:
                # Gọi API Inference thời gian thực (Độ trễ < 50ms)
                response = self.endpoint.predict(instances=instances)
                prediction_result = response.predictions[0]
            else:
                # Mock prediction nếu chưa deploy Vertex model
                prediction_result = "Tăng giá lừa đảo (Pump fake)" if price > 50000 else "Bình thường"

            logger.info(f"[Inference] {symbol} | Price: {price} -> Vertex AI Dự đoán: {prediction_result}")
            
            # Ghi nhận message đã xử lý
            message.ack()

        except Exception as e:
            logger.error(f"Lỗi xử lý ML realtime: {e}")
            message.nack() # Kích hoạt cơ chế gửi lại (Retry)

    def start_streaming(self):
        logger.info(f"Bắt đầu lắng nghe Streams tại {self.subscription_path} và Inference với Vertex AI...")
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            logger.info("Dừng Listener.")

def main():
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        logger.error("Vui lòng thiết lập biếm môi trường GOOGLE_APPLICATION_CREDENTIALS")
        return
        
    predictor = RealTimeVertexAIPredictor()
    predictor.start_streaming()

if __name__ == "__main__":
    main()
