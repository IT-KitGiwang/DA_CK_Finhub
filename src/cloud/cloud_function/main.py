import base64
import json
import time
import os
import logging
import functions_framework
from google.cloud import bigquery

# Tối ưu hóa Serverless: Khởi tạo Client ở Global Namespace 
# để tái sử dụng Connection giữa các lần kích hoạt (Warm booting), tiết kiệm độ trễ khởi tạo.
bq_client = None

MAX_FUTURE_SECONDS = 86400      # 24 giờ
MAX_PAST_SECONDS   = 2592000    # 30 ngày

def get_valid_symbols():
    """Lấy danh sách các đồng Coin hợp lệ từ System Env (Single Source of Truth)"""
    raw_symbols = os.getenv("SYMBOLS", "BINANCE:BTCUSDT,BINANCE:ETHUSDT,BINANCE:BNBUSDT,BINANCE:SOLUSDT,BINANCE:DOGEUSDT")
    return [sym.strip() for sym in raw_symbols.split(",") if sym.strip()]

@functions_framework.http
def clean_and_insert_crypto(request):
    """
    Cloud Run HTTP Function - Triggered by Pub/Sub via Eventarc.
    Eventarc gửi Pub/Sub messages dưới dạng HTTP POST request.
    Thực hiện 6 BƯỚC DATA CLEANING và Push Real-time Streaming thẳng vào BigQuery.
    """
    global bq_client
    if not bq_client:
        bq_client = bigquery.Client()
        
    # Cấu hình Kiến trúc từ Biến môi trường của Function
    gcp_project = os.getenv("GCP_PROJECT_ID", "your_project_id")
    bq_dataset = os.getenv("GCP_BQ_DATASET", "finhub_dataset")
    bq_table_name = os.getenv("GCP_BQ_TABLE_STREAM", "crypto_trades")
    table_id = f"{gcp_project}.{bq_dataset}.{bq_table_name}"
    
    valid_symbols = get_valid_symbols()

    try:
        # ===== BƯỚC 1: GIẢI MÃ TIN NHẮN PUB/SUB TỪ HTTP REQUEST =====
        # Eventarc gửi Pub/Sub message dưới dạng JSON trong body HTTP POST
        envelope = request.get_json(silent=True)
        if not envelope:
            logging.warning("❌ Request body rỗng hoặc không phải JSON. Bỏ qua.")
            return ("Bad Request: no JSON payload", 400)

        # Pub/Sub bọc data trong envelope.message.data (base64 encoded)
        pubsub_message = envelope.get("message", {})
        encoded_data = pubsub_message.get("data")
        
        if not encoded_data:
            # Fallback: Thử lấy trực tiếp từ envelope.data (một số cấu hình khác)
            encoded_data = envelope.get("data")
        
        if not encoded_data:
            logging.warning("❌ Pub/Sub message không chứa Payload 'data'. Bỏ qua.")
            return ("Bad Request: no data field", 400)

        raw_message = base64.b64decode(encoded_data).decode('utf-8')
        data = json.loads(raw_message)
        
        # ===== BƯỚC 2: BẮT LỖI NULL (Loại bỏ giao dịch khuyết dữ liệu) =====
        if not all(k in data for k in ("time", "symbol", "price", "volume")):
            logging.warning("❌ LỖI CLEANSING: Dữ liệu bị khuyết trường cốt lõi. Giao dịch bị vứt bỏ.")
            return ("OK - skipped: missing fields", 200)
            
        # Chuẩn hóa Chuỗi Data Thô (Trim khoảng trắng & Viết hoa chuẩn Name)
        symbol = str(data['symbol']).strip().upper()
        price = float(data['price'])
        volume = float(data['volume'])
        trade_timestamp_ms = int(data['time'])   # UNIX timestamp millisecond API
        
        # ===== BƯỚC 3: BỘ LỌC DANH SÁCH TRẮNG (Whitelist) =====
        if symbol not in valid_symbols:
            logging.info(f"⚠️ BỎ QUA: Bắt được tín hiệu {symbol} nhưng không nằm trong Whitelist đã cấp phép.")
            return ("OK - skipped: not in whitelist", 200)
            
        # ===== BƯỚC 4: VALIDATION TOÁN HỌC (Chặn giá trị Âm/0) =====
        if price <= 0 or volume <= 0:
            logging.warning(f"❌ LỖI VẬT LÝ: Giao dịch của {symbol} có khối lượng/giá nhỏ hơn bằng 0.")
            return ("OK - skipped: invalid price/volume", 200)
            
        # ===== BƯỚC 5: VALIDATION THỜI GIAN (Drift Protection) =====
        current_time_ms = int(time.time() * 1000)
        future_limit = current_time_ms + (MAX_FUTURE_SECONDS * 1000)
        past_limit = current_time_ms - (MAX_PAST_SECONDS * 1000)
        
        if trade_timestamp_ms > future_limit or trade_timestamp_ms < past_limit:
            logging.warning(f"❌ LỖI LỆCH THỜI GIAN: Giao dịch {symbol} vi phạm giới hạn tương lai/quá khứ.")
            return ("OK - skipped: timestamp out of range", 200)
            
        # ===== BƯỚC 6: XẢ SINK VÀO BIGQUERY (SILVER LAYER) =====
        row_to_insert = [
            {
                "time": trade_timestamp_ms / 1000.0,
                "symbol": symbol,
                "price": price,
                "volume": volume
            }
        ]
        
        # Bắn API Insert Streaming
        errors = bq_client.insert_rows_json(table_id, row_to_insert)
        if errors:
            logging.error(f"❌ THẤT BẠI: Lỗi khi Bắn API vào BigQuery: {errors}")
            return (f"Error inserting to BigQuery: {errors}", 500)
        else:
            logging.info(f"✅ THÀNH CÔNG [REAL-TIME]: Chèn {symbol} (Vol: {volume} | Price: {price}$) thành công.")
            return ("OK", 200)

    except json.JSONDecodeError:
        logging.error("❌ LỖI FORMAT: Chuỗi dữ liệu từ Pub/Sub không chuẩn định dạng JSON.")
        return ("Bad Request: invalid JSON in message", 400)
    except Exception as e:
        logging.critical(f"🔥 SẬP CLOUD FUNCTION: Phát hiện lỗi nghiêm trọng - {e}")
        return (f"Internal Server Error: {e}", 500)
