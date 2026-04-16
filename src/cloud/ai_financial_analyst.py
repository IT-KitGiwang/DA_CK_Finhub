"""
Module: AI Financial Analyst (Bot môi giới tư vấn bằng Trí Tuệ Nhân Tạo)
Mô tả:
 - Sử dụng gói thư viện Google Cloud BigQuery truy vấn giá hoặc hiện tượng bất thường (Anomaly/Pump/Dump).
 - Gom dữ liệu thô ấy thành dạng Ngữ cảnh (Context) Prompt Engineering và hỏi OpenRouter AI.
 - Trả ra kết quả phân loại định tính và gợi ý hành động Buy/Sell như một chuyên gia tư vấn.
"""

import os
import requests
import json
import logging
import sys
from google.cloud import bigquery

# 1. Cấu hình Logging đầy đủ theo luật Rule Code
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [AI-ANALYST] %(levelname)s: %(message)s",
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
            logger.warning(f"Không tìm được file env ở đường dẫn {path}.")
    except Exception as e:
        logger.error(f"Lỗi khi đọc file cấu hình .env: {e}")

class AIFinancialAnalyst:
    def __init__(self):
        """Khởi tạo và check biến môi trường bảo mật cần thiết cho BQ và API AI"""
        logger.info("Tiến hành cấu hình Bot Tư Vấn AI Cấp Cao...")
        try:
            self.project_id = os.getenv("GCP_PROJECT_ID")
            self.dataset_id = os.getenv("GCP_BQ_DATASET")
            self.openrouter_api_key = os.getenv("OPENROUTER_API_KEY")
            
            # Khóa điều kiện tiên quyết
            if not self.openrouter_api_key:
                raise ValueError("OPENROUTER_API_KEY chưa được cấu hình. Ai sẽ trả lời phân tích đây?")
            if not self.project_id or not self.dataset_id:
                raise ValueError("Thiếu biến Project ID và Dataset cho BigQuery. Không tìm thấy nguồn dữ liệu.")

            # Kết nối API Google BigQuery
            logger.info(f"Đang bám cầu kết nối BigQuery Client với Project ID: {self.project_id}")
            self.bq_client = bigquery.Client(project=self.project_id)
        
        except ValueError as ve:
            logger.error(f"Lỗi nhập liêu cấu hình môi trường: {ve}")
            self.bq_client = None
            raise
        except Exception as e:
            logger.error(f"Lỗi hệ thống khi móc nối Google BigQuery (Sẽ trả kết quả MOCK DEMO): {e}")
            self.bq_client = None

    def fetch_latest_market_insight(self) -> str:
        """Kéo 5 bản ghi giá sát thời điểm hiện tại nhất làm Đầu Vào (Context) cho mô hình ngôn ngữ LLM"""
        # Trả về kết quả ngẫu nhiên nếu không móc được Backend BQ
        if not self.bq_client:
            logger.warning("Do BQ_Client không tồn tại. Mock 1 tin hiệu khẩn cấp giả lập.")
            return "BINANCE:BTCUSDT giá hiện tại 65400, trong 5 phút qua bị BQML đánh dấu là ANOMALY với volume đột biến 300%."

        query = f"""
        SELECT symbol, price, volume, timestamp 
        FROM `{self.project_id}.{self.dataset_id}.{os.getenv("GCP_BQ_TABLE_STREAM", "streaming_crypto_trades")}`
        ORDER BY timestamp DESC LIMIT 5
        """
        
        logger.info("Đang truy vấn thông số thị trường Tick data thật từ BigQuery...")
        try:
            results_df = self.bq_client.query(query).to_dataframe()
            if results_df.empty:
                logger.warning("Bảng BigQuery rỗng! Không có dòng dữ liệu báo cáo nào!")
                return "Không có dữ liệu mới để phân tích"
                
            # Đóng hộp gói data cho GPT (Sử dụng Json list)
            insight_data = results_df.to_json(orient='records')
            logger.info("Hoàn tất lấy Data Insight thô.")
            return insight_data
            
        except Exception as e:
            logger.error(f"Câu lệnh query BQ bị tạch (Failed to query BQ): {e}", exc_info=True)
            return "Lỗi Server, không truy xuất được database BigQuery."

    def analyze_with_ai(self, context_data: str):
        """Hàm thiết kế Cấu trúc Prompt (Prompt Engineering) và Submit Request gọi Robot Trí Tuệ (LLM) đắt tiền."""
        logger.info("📡 Đang gửi câu lệnh đánh giá thị trường (Prompt) sang não bộ AI (Claude 3.5 / OpenRouter)...")
        
        prompt = f"""
Bạn là chuyên gia phân tích tài chính (Quants) cấp bậc Senior. 
Dưới đây là một mẫu dữ liệu Streaming (Tick data) và đánh giá Anomaly xuất từ Hệ thống BigQuery ML của tập đoàn Data Tech cho các cặp tiền điện tử (Crypto):

DỰ LIỆU NHẬN ĐƯỢC MỚI NHẤT TRONG KHOẢNG KHẮC NÀY:
{context_data}

Yêu cầu nhiệm vụ:
1. Đọc dữ liệu biến động giá trị khối lượng (Volume) và Giá theo dòng thời gian từ mảng JSON kia.
2. Trích xuất ngay lập tức nhận định: Đang diễn ra tích luỹ (Accumulation), hay một vụ bơm xả hàng (Pump/Dump)?
3. Đưa ra Cảnh báo Đầu Tư rủi ro / Lời khuyên Mua-Bán (Buy/Sell/Hold) cho giao dịch viên.
Yêu cầu định dạng: Viết cực kỳ đi thẳng vào trọng tâm, không chào hỏi, viết 2 dòng ngắn gọn.
"""

        try:
            response = requests.post(
                url="https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.openrouter_api_key}",
                    "HTTP-Referer": "https://finhub.crypto.io", 
                    "X-Title": "Real Estate Finhub AI Analytics",
                    "Content-Type": "application/json"
                },
                data=json.dumps({
                    "model": "anthropic/claude-3-sonnet", # AI Model cực thông minh được ưu chuộng
                    "messages": [
                        {"role": "user", "content": prompt}
                    ]
                }),
                timeout=15 # Time-out sau 15 giây tránh treo cổng mạng
            )
            
            # Xử lý kết quả trả về bằng hàm if-else bắt Response Code chuẩn API
            if response.status_code == 200:
                ai_reply = response.json().get("choices", [{}])[0].get("message", {}).get("content", "Không có nội dung trả lời.")
                
                logger.info("=== 📈 BÁO CÁO PHÂN TÍCH RA ĐỜI BỞI AI ===")
                # In ra Report để người dùng / frontend nhìn thấy.
                print(f"\n{ai_reply}\n")
                logger.info("==========================================")
            else:
                logger.error(f"OpenRouter API đánh lỗi trả về mã HTTP {response.status_code} - Header lỗi {response.text}")
                
        except requests.exceptions.Timeout:
            logger.error("🛑 Mạng phản hồi quá chậm! Máy chủ OpenRouter đã bị Timeout.")
        except requests.exceptions.RequestException as e:
            logger.error(f"🛑 Đường truyền API bị đứt kết nối vật lý / Proxy: {e}")
        except Exception as e:
            logger.critical(f"🛑 Bị gãy tiến trình bất thình lình khi tương tác API AI: {e}")

def main():
    try:
        # 1. Trình gọi siêu cấu trúc load env
        load_dotenv_file()
        
        # 2. Sinh thể đối tượng Robot
        analyst = AIFinancialAnalyst()
        
        # 3. Kéo data
        insight_data = analyst.fetch_latest_market_insight()
        
        # 4. Tư duy và viết Report
        analyst.analyze_with_ai(insight_data)
        
    except Exception as e:
        logger.critical(f"Chương trình chính Main Application bị đánh sập: {e}")

if __name__ == "__main__":
    main()
