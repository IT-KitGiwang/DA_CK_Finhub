import base64
import json
import os
import logging
import uuid
from datetime import datetime, timezone, timedelta
import functions_framework
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound

# Khởi tạo Client ở Global Namespace để tận dụng Warm Boot
bq_client = None
storage_client = None
_table_verified = False  # Flag kiểm tra bảng đã tồn tại chưa (chỉ check 1 lần/cold start)



def get_valid_symbols():
    raw_symbols = os.getenv("SYMBOLS", "BINANCE:BTCUSDT,BINANCE:ETHUSDT,BINANCE:BNBUSDT,BINANCE:SOLUSDT")
    return [sym.strip() for sym in raw_symbols.split(",") if sym.strip()]

# Ánh xạ tên đồng tiền chuẩn (Dimension Mapping trực tiếp)
ASSET_MAPPING = {
    "BINANCE:BTCUSDT": "Bitcoin",
    "BINANCE:ETHUSDT": "Ethereum",
    "BINANCE:BNBUSDT": "BNB",
    "BINANCE:SOLUSDT": "Solana"
}


def ensure_bq_table(client, table_id, dataset_id, project_id):
    """
    Tự động tạo Dataset + Table trong BigQuery nếu chưa tồn tại.
    Chỉ chạy 1 lần mỗi cold start nhờ flag _table_verified.
    CHÚ Ý: Chỉ bắt NotFound — mọi lỗi khác (403 Permission, 500 Server)
    sẽ được nổi lên để caller xử lý, tránh nuốt lỗi nghiêm trọng.
    """
    global _table_verified
    if _table_verified:
        return

    # FIX BUG #1: Chỉ bắt NotFound thay vì Exception chung
    # Đảm bảo Dataset tồn tại
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-southeast1"
        client.create_dataset(dataset, exists_ok=True)
        logging.info(f"📦 Đã tạo dataset: {dataset_id}")

    # Đảm bảo Table tồn tại với schema đúng
    try:
        client.get_table(table_id)
    except NotFound:
        schema = [
            bigquery.SchemaField("time", "DATETIME", mode="REQUIRED", description="Thời điểm giao dịch gốc"),
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED", description="Mã giao dịch (VD: BINANCE:BTCUSDT)"),
            bigquery.SchemaField("asset_name", "STRING", mode="NULLABLE", description="Tên đầy đủ (VD: Bitcoin)"),
            bigquery.SchemaField("price", "FLOAT64", mode="REQUIRED", description="Giá giao dịch (USD)"),
            bigquery.SchemaField("volume", "FLOAT64", mode="REQUIRED", description="Khối lượng giao dịch"),
            bigquery.SchemaField("year", "INT64", mode="NULLABLE", description="Năm giao dịch"),
            bigquery.SchemaField("month", "INT64", mode="NULLABLE", description="Tháng giao dịch"),
            bigquery.SchemaField("day", "INT64", mode="NULLABLE", description="Ngày giao dịch"),
            bigquery.SchemaField("processed_at", "DATETIME", mode="NULLABLE", description="Thời điểm xử lý trên Cloud (ISO 8601)"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True)
        logging.info(f"📦 Đã tạo bảng BigQuery: {table_id}")

    _table_verified = True

@functions_framework.http
def clean_and_insert_crypto(request):
    global bq_client, storage_client, _table_verified
    
    if not bq_client:
        bq_client = bigquery.Client()
    if not storage_client:
        storage_client = storage.Client()
        
    # Lấy cấu hình từ Environment
    gcp_project = os.getenv("GCP_PROJECT_ID", "phan-tich-du-lieu-lon")
    bq_dataset = os.getenv("GCP_BQ_DATASET", "finhub_dw")
    bq_table_name = os.getenv("GCP_BQ_TABLE_STREAM", "streaming_crypto_trades")
    gcs_bucket_name = os.getenv("GCP_GCS_BUCKET", "raw_data_api_ptdlnhom15")
    
    table_id = f"{gcp_project}.{bq_dataset}.{bq_table_name}"
    valid_symbols = get_valid_symbols()

    try:
        # 1. Parse Pub/Sub envelope
        envelope = request.get_json(silent=True)
        if not envelope:
            return ("Bad Request: no JSON payload", 400)

        pubsub_message = envelope.get("message", {})
        encoded_data = pubsub_message.get("data") or envelope.get("data")
        
        # Lấy Publish Time tự nhiên của hệ thống Pub/Sub làm gốc thời gian xử lý
        publish_time_str = pubsub_message.get("publishTime")
        if publish_time_str:
            try:
                dt_object = datetime.fromisoformat(publish_time_str.replace('Z', '+00:00'))
            except ValueError:
                dt_object = datetime.now(timezone.utc)
        else:
            dt_object = datetime.now(timezone.utc)
            
        # Nâng lên timezone Việt Nam (+7) để đồng bộ hoàn toàn với Producer
        dt_vn = dt_object + timedelta(hours=7)
        
        # Format chuẩn sạch cho BigQuery TIMESTAMP (không dư microseconds/UTC text)
        processed_at_val = dt_vn.strftime("%Y-%m-%d %H:%M:%S")
        
        if not encoded_data:
            return ("Bad Request: no data field", 400)

        raw_message = base64.b64decode(encoded_data).decode('utf-8')
        data = json.loads(raw_message)
        
        # 2. Data Cleaning
        if not all(k in data for k in ("time", "symbol", "price", "volume")):
            return ("OK - skipped: missing fields", 200)
        
        # Parse time string từ Producer (format: '%Y-%m-%d %H:%M:%S') thành BQ TIMESTAMP
        trade_time_str = str(data['time']).strip()
        try:
            trade_dt = datetime.strptime(trade_time_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return ("OK - skipped: invalid time format", 200)
        # Format chuẩn cho BigQuery TIMESTAMP
        bq_time_val = trade_dt.strftime("%Y-%m-%d %H:%M:%S")

        symbol = str(data['symbol']).strip().upper()
        price = float(data['price'])
        volume = float(data['volume'])
        
        # Filter & Validate
        if symbol not in valid_symbols:
            return ("OK - skipped: not in whitelist", 200)
        if price <= 0 or volume <= 0:
            return ("OK - skipped: invalid values", 200)

        clean_record = {
            "time": bq_time_val,  # Gốc 100% từ producer, đã validate & format
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "processed_at": processed_at_val
        }

        # Bản ghi chuẩn bị đẩy lên BigQuery
        bq_record = clean_record.copy()
        bq_record.update({
            "asset_name": ASSET_MAPPING.get(symbol, symbol),
            "year": trade_dt.year,    # Dùng Giờ Khớp lệnh thực tế thay vì Giờ Xử lý Cloud
            "month": trade_dt.month,
            "day": trade_dt.day
        })

        # ===== NHÁNH 1: BIGQUERY (Streaming) =====
        ensure_bq_table(bq_client, table_id, bq_dataset, gcp_project)
        try:
            errors = bq_client.insert_rows_json(table_id, [bq_record])
            if errors:
                logging.error(f"BQ Error: {errors}")
        except NotFound:
            logging.warning("⚠️ BQ Dataset/Table not found! Resetting state and retrying...")
            _table_verified = False
            ensure_bq_table(bq_client, table_id, bq_dataset, gcp_project)
            errors = bq_client.insert_rows_json(table_id, [bq_record])
            if errors:
                logging.error(f"BQ Error on retry: {errors}")

        # ===== NHÁNH 2: CLOUD STORAGE (Data Lake Sink) =====
        # FIX BUG #3: Tách riêng try/except cho GCS — nếu GCS lỗi, vẫn return 200
        # để Pub/Sub KHÔNG retry (vì BQ đã insert thành công ở trên).
        try:
            # Tách thư mục phân mảnh bằng Giờ Giao Dịch Thực (Event Time - trade_dt)
            partition_path = trade_dt.strftime("clean_data/year=%Y/month=%m/day=%d")
            filename = f"{partition_path}/{symbol}_{int(trade_dt.timestamp() * 1000)}_{uuid.uuid4().hex[:8]}.json"
            
            bucket = storage_client.bucket(gcs_bucket_name)
            blob = bucket.blob(filename)
            blob.upload_from_string(json.dumps(clean_record), content_type='application/json')
            logging.info(f"✅ REAL-TIME SYNC: {symbol} -> BigQuery & GCS ({filename})")
        except Exception as gcs_err:
            # Log lỗi GCS nhưng KHÔNG crash function — BQ data đã an toàn
            logging.error(f"⚠️ GCS Upload failed (BQ vẫn OK): {gcs_err}")

        return ("OK", 200)

    except ValueError as ve:
        logging.warning(f"⚠️ Data không hợp lệ, bỏ qua: {ve}")
        return ("OK - skipped: invalid data format", 200)

    except Exception as e:
        logging.critical(f"🔥 SẬP: {e}", exc_info=True)
        return (f"Internal Error: {e}", 500)
