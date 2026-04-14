"""
OpenRouter AI Financial Analyst bot
Connects to BigQuery to fetch the latest Streaming Data & ML Predictions,
Formulates a context, and strictly evaluates it using OpenRouter's LLMs 
(e.g., Anthropic Claude / OpenAI GPT-4) to generate Automated Trading Reports.
"""

import os
import requests
import json
import logging
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [AI-ANALYST] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = "../../.env") -> None:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line: continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

class AIFinancialAnalyst:
    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.dataset_id = os.getenv("GCP_BQ_DATASET")
        self.openrouter_api_key = os.getenv("OPENROUTER_API_KEY")
        
        if not self.openrouter_api_key:
            raise ValueError("OPENROUTER_API_KEY chưa được cấu hình trong .env")

        try:
            # Requires GOOGLE_APPLICATION_CREDENTIALS in env to work
            self.bq_client = bigquery.Client(project=self.project_id)
        except Exception as e:
            logger.warning(f"Lỗi khởi tạo BQ Client (sẽ dùng bộ dữ liệu giả lập cho Demo): {e}")
            self.bq_client = None

    def fetch_latest_market_insight(self) -> str:
        """Kéo dữ liệu Anomaly hoặc giá mới nhất từ DataWarehouse"""
        if not self.bq_client:
            # Mock return nếu chưa cắm GCP keys
            return "BINANCE:BTCUSDT giá hiện tại 65400, trong 5 phút qua bị BQML đánh dấu là ANOMALY với volume đột biến 300%."

        query = f"""
        SELECT symbol, price, volume 
        FROM `{self.project_id}.{self.dataset_id}.streaming_crypto_trades`
        ORDER BY timestamp DESC LIMIT 5
        """
        try:
            results = self.bq_client.query(query).to_dataframe()
            # Serialize for LLM prompt
            return results.to_json(orient='records')
        except Exception as e:
            logger.error(f"Failed to query BQ: {e}")
            return "Lỗi truy vấn, không có dữ liệu."

    def analyze_with_ai(self, context_data: str):
        """Đẩy dữ liệu qua OpenRouter API để Phân tích Tự Động"""
        logger.info("Đang gọi OpenRouter AI (Claude 3.5 / GPT-4o)...")
        
        prompt = f"""
Bạn là chuyên gia phân tích tài chính (Quants) cấp bậc Senior. 
Dưới đây là một mẫu dữ liệu Streaming (Tick data) hoặc báo cáo Anomaly Detection xuất từ Hệ thống BigQuery ML cho các cặp tiền điện tử / chứng khoán:

DỰ LIỆU NHẬN ĐƯỢC MỚI NHẤT:
{context_data}

Yêu cầu nhiệm vụ:
1. Đọc dữ liệu biến động giá trị khối lượng (Volume) và Giá.
2. Trích xuất ngay lập tức nhận định: Đang diễn ra tích luỹ, hay một vụ xả hàng (Dump/Pump)?
3. Đưa ra Cảnh báo Đầu Tư cho người dùng trên biểu đồ. (Viết cực kỳ ngắn gọn, định dạng rõ ràng).
"""

        response = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {self.openrouter_api_key}",
                "HTTP-Referer": "https://finhub.crypto.io", 
                "X-Title": "Real Estate Finhub AI Analytics",
                "Content-Type": "application/json"
            },
            data=json.dumps({
                "model": "anthropic/claude-3-sonnet", # Bạn có thể đổi sang model Llama 3 theo ý muốn trên OpenRouter
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            })
        )
        
        if response.status_code == 200:
            ai_reply = response.json()["choices"][0]["message"]["content"]
            logger.info("=== 📈 BÁO CÁO PHÂN TÍCH RA ĐỜI BỞI AI ===")
            print(ai_reply)
            logger.info("==========================================")
        else:
            logger.error(f"OpenRouter API thất bại: {response.status_code} - {response.text}")

def main():
    load_dotenv_file()
    analyst = AIFinancialAnalyst()
    insight_data = analyst.fetch_latest_market_insight()
    analyst.analyze_with_ai(insight_data)

if __name__ == "__main__":
    main()
