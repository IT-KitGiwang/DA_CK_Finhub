import base64
import json
import time
import os
import logging
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

def clean_and_insert_crypto(event, context):
    """
    Cloud Function Triggered by Pub/Sub.
    Thực hiện 6 BƯỚC DATA CLEANING (Thế thân cho Spark GCP) và Push Real-time Streaming thẳng vào BigQuery.
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
        # Bước 1: Giải mã tin nhắn Pub/Sub (từ chuỗi mã hóa Base64 -> String -> JSON)
        if 'data' not in event:
            logging.warning("Pub/Sub message không chứa Payload 'data'. Bỏ qua.")
            return

        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(pubsub_message)
        
        # Bước 2: Bắt lỗi Null (Loại bỏ các giao dịch bị khuyết dữ liệu quan trọng)
        if not all(k in data for k in ("time", "symbol", "price", "volume")):
            logging.warning("❌ LỖI CLEANSING: Dữ liệu bị khuyết trường cốt lõi. Giao dịch bị vứt bỏ.")
            return
            
        # Chuẩn hóa Chuỗi Data Thô (Trim khoảng trắng & Viết hoa chuẩn Name)
        symbol = str(data['symbol']).strip().upper()
        price = float(data['price'])
        volume = float(data['volume'])
        trade_timestamp_ms = int(data['time'])   # UNIX timestamp millisecond API
        
        # Bước 3: Bộ lọc Danh Sách Trắng (Whitelist)
        if symbol not in valid_symbols:
            logging.info(f"⚠️ BỎ QUA: Bắt được tín hiệu {symbol} nhưng không nằm trong Whitelist đã cấp phép.")
            return
            
        # Bước 4: Validation Toán Học (Chặn giá trị Âm/0)
        if price <= 0 or volume <= 0:
            logging.warning(f"❌ LỖI VẬT LÝ: Giao dịch của {symbol} có khối lượng/giá nhỏ hơn bằng 0. Có thể là mã độc API.")
            return
            
        # Bước 5: Validation Thời gian (Bảo vệ đồng hồ trôi dạt - Drift Protection)
        current_time_ms = int(time.time() * 1000)
        future_limit = current_time_ms + (MAX_FUTURE_SECONDS * 1000)
        past_limit = current_time_ms - (MAX_PAST_SECONDS * 1000)
        
        if trade_timestamp_ms > future_limit or trade_timestamp_ms < past_limit:
            logging.warning(f"❌ LỖI LỆCH THỜI GIAN: Giao dịch {symbol} vi phạm giới hạn tương lai/quá khứ.")
            return
            
        # BƯỚC 6: XẢ SINK VÀO BIGQUERY (SILVER LAYER) 
        # (Lưu ý: Mảng deduplication sẽ được tính toán trực tiếp trên View của Bigquery để tối ưu năng lực luồng Stream)
        
        row_to_insert = [
            {
                "time": trade_timestamp_ms / 1000.0, # Ép kiểu chuẩn Double Second cho Timestamp của GCP SQL
                "symbol": symbol,
                "price": price,
                "volume": volume
            }
        ]
        
        # Bắn API Insert Streaming (Sôi động Real-time)
        errors = bq_client.insert_rows_json(table_id, row_to_insert)
        if errors:
            logging.error(f"❌ THẤT BẠI: Lỗi khi Bắn API vào BigQuery: {errors}")
        else:
            logging.info(f"✅ THÀNH CÔNG [REAL-TIME]: Chèn {symbol} (Vol: {volume} | Price: {price}$) qua mặt mây thành công.")

    except json.JSONDecodeError:
        logging.error("❌ LỖI FORMAT: Chuỗi dữ liệu từ Pub/Sub không chuẩn định dạng JSON.")
    except Exception as e:
        logging.critical(f"🔥 SẬP CLOUD FUNCTION: Phát hiện lỗi nghiêm trọng Chưa Lường Trước - {e}")
