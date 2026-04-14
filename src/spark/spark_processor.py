"""
Spark Streaming Processor with Comprehensive Data Cleaning
===========================================================
Pipeline:  Finnhub WebSocket → Kafka → [SPARK CLEANING] → Hive (HDFS / Parquet)
Layer:     Bronze (Raw Kafka) → Silver (Cleaned) → Ghi vào bảng crypto_trades

Quy trình làm sạch gồm 7 bước:
  1. Schema Validation    — Ép kiểu dữ liệu chuẩn
  2. Null Elimination     — Loại bỏ dòng thiếu dữ liệu
  3. Symbol Whitelist     — Chỉ chấp nhận symbol hợp lệ
  4. Price Validation     — Lọc giá bất hợp lý (<=0, outlier)
  5. Volume Validation    — Lọc khối lượng bất hợp lý
  6. Timestamp Validation — Loại bỏ thời gian không hợp lệ
  7. Deduplication        — Xóa dữ liệu trùng lặp trong micro-batch
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, trim, upper,
    current_timestamp, unix_timestamp, lit, when,
    window, count as spark_count
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging

# ─────────────────────────────────────────────────────────────────────────────
# Cấu hình Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Hằng số: Danh sách symbol hợp lệ và ngưỡng lọc outlier
# ─────────────────────────────────────────────────────────────────────────────

# Chỉ chấp nhận các symbol đã đăng ký trong .env
VALID_SYMBOLS = [
    "BINANCE:BTCUSDT",
    "BINANCE:ETHUSDT",
    "BINANCE:BNBUSDT"
]

# Ngưỡng giá hợp lý cho từng đồng (tránh lỗi API gửi giá 0 hoặc 999999999)
# Cập nhật khi thị trường biến động mạnh
PRICE_BOUNDS = {
    "BINANCE:BTCUSDT": {"min": 1000,   "max": 500000},   # BTC: $1K - $500K
    "BINANCE:ETHUSDT": {"min": 50,     "max": 50000},    # ETH: $50  - $50K
    "BINANCE:BNBUSDT": {"min": 10,     "max": 5000},     # BNB: $10  - $5K
}

# Khối lượng giao dịch tối đa hợp lý (tránh lỗi gửi volume = 999999)
MAX_VOLUME = {
    "BINANCE:BTCUSDT": 1000,    # Tối đa 1000 BTC / giao dịch
    "BINANCE:ETHUSDT": 50000,   # Tối đa 50K ETH / giao dịch
    "BINANCE:BNBUSDT": 100000,  # Tối đa 100K BNB / giao dịch
}

# Thời gian tối đa chấp nhận: không quá 24 giờ trong tương lai, không quá 30 ngày trong quá khứ
MAX_FUTURE_SECONDS = 86400      # 24 giờ
MAX_PAST_SECONDS   = 2592000    # 30 ngày


# =============================================================================
# HÀM LÀM SẠCH DỮ LIỆU (DATA CLEANING PIPELINE)
# =============================================================================

def clean_data(raw_df: DataFrame) -> DataFrame:
    """
    Pipeline làm sạch dữ liệu gồm 7 bước tuần tự.
    Mỗi bước loại bỏ một dạng dữ liệu bẩn khác nhau.

    Input:  raw_df — DataFrame thô đã parse JSON từ Kafka
    Output: cleaned_df — DataFrame sạch sẵn sàng ghi vào Hive
    """

    # ─────────────────────────────────────────────────────────────────────
    # BƯỚC 1/7: LOẠI BỎ GIÁ TRỊ NULL (Null Elimination)
    # Nếu bất kỳ cột nào bị null → Xóa cả dòng
    # Nguyên nhân: Finnhub đôi khi gửi JSON thiếu field khi mạng lag
    # ─────────────────────────────────────────────────────────────────────
    step1_df = raw_df.dropna(
        how="any",
        subset=["time", "symbol", "price", "volume"]
    )

    # ─────────────────────────────────────────────────────────────────────
    # BƯỚC 2/7: CHUẨN HÓA CHUỖI (String Normalization)
    # Loại bỏ khoảng trắng thừa đầu/cuối và chuẩn hóa viết HOA
    # Phòng trường hợp API trả về " binance:btcusdt " thay vì "BINANCE:BTCUSDT"
    # ─────────────────────────────────────────────────────────────────────
    step2_df = step1_df \
        .withColumn("symbol", upper(trim(col("symbol"))))

    # ─────────────────────────────────────────────────────────────────────
    # BƯỚC 3/7: LỌC SYMBOL HỢP LỆ (Symbol Whitelist)
    # Chỉ giữ lại các đồng tiền đã đăng ký
    # Ngăn chặn dữ liệu lạ xâm nhập vào hệ thống
    # ─────────────────────────────────────────────────────────────────────
    step3_df = step2_df.filter(
        col("symbol").isin(VALID_SYMBOLS)
    )

    # ─────────────────────────────────────────────────────────────────────
    # BƯỚC 4/7: LỌC GIÁ BẤT HỢP LÝ (Price Validation)
    # Giá phải > 0 và nằm trong khoảng hợp lý cho từng đồng tiền
    # Phòng trường hợp API lỗi gửi price = 0 hoặc giá cực đoan
    # ─────────────────────────────────────────────────────────────────────
    price_condition = lit(False)
    for symbol, bounds in PRICE_BOUNDS.items():
        price_condition = price_condition | (
            (col("symbol") == symbol) &
            (col("price") >= bounds["min"]) &
            (col("price") <= bounds["max"])
        )
    step4_df = step3_df.filter(price_condition)

    # ─────────────────────────────────────────────────────────────────────
    # BƯỚC 5/7: LỌC KHỐI LƯỢNG BẤT HỢP LÝ (Volume Validation)
    # Volume phải > 0 và không vượt ngưỡng tối đa cho từng đồng
    # Phòng trường hợp giao dịch ảo hoặc lỗi dữ liệu
    # ─────────────────────────────────────────────────────────────────────
    volume_condition = lit(False)
    for symbol, max_vol in MAX_VOLUME.items():
        volume_condition = volume_condition | (
            (col("symbol") == symbol) &
            (col("volume") > 0) &
            (col("volume") <= max_vol)
        )
    step5_df = step4_df.filter(volume_condition)

    # ─────────────────────────────────────────────────────────────────────
    # BƯỚC 6/7: LỌC THỜI GIAN BẤT HỢP LÝ (Timestamp Validation)
    # Thời gian giao dịch không được nằm quá xa trong tương lai
    # hoặc quá xa trong quá khứ (tránh dữ liệu cũ bị replay)
    # ─────────────────────────────────────────────────────────────────────
    step6_df = step5_df.filter(
        # Không quá 24 giờ trong tương lai
        (unix_timestamp(col("time")) <= unix_timestamp(current_timestamp()) + MAX_FUTURE_SECONDS) &
        # Không quá 30 ngày trong quá khứ
        (unix_timestamp(col("time")) >= unix_timestamp(current_timestamp()) - MAX_PAST_SECONDS)
    )

    # ─────────────────────────────────────────────────────────────────────
    # BƯỚC 7/7: LOẠI BỎ TRÙNG LẶP TRONG MICRO-BATCH (Deduplication)
    # Khi Kafka bị lag hoặc producer retry, cùng 1 giao dịch có thể
    # được gửi 2-3 lần. Dùng dropDuplicates để giữ lại duy nhất 1 bản.
    # ─────────────────────────────────────────────────────────────────────
    step7_df = step6_df.dropDuplicates(["time", "symbol", "price", "volume"])

    logger.info("Data Cleaning Pipeline completed — 7/7 steps applied successfully.")
    return step7_df


# =============================================================================
# HÀM CHÍNH: KHỞI CHẠY PIPELINE
# =============================================================================

def main() -> None:
    logger.info("Initializing Spark Session for Hive & Kafka streaming...")

    # ─────────────────────────────────────────────────────────────────────
    # Khởi tạo Spark Session tương thích với Hive Metastore 4.0.0
    # ─────────────────────────────────────────────────────────────────────
    spark = SparkSession.builder \
        .appName("CryptoKafkaToHive") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://master:9083") \
        .config("spark.sql.hive.metastore.version", "4.0.0") \
        .config("spark.sql.hive.metastore.jars", "path") \
        .config("spark.sql.hive.metastore.jars.path", "file:///opt/hive/lib/*") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ─────────────────────────────────────────────────────────────────────
    # Định nghĩa Schema ứng với cấu trúc JSON từ Finnhub Producer
    # ─────────────────────────────────────────────────────────────────────
    schema = StructType([
        StructField("time",   StringType(),  True),
        StructField("symbol", StringType(),  True),
        StructField("price",  DoubleType(),  True),
        StructField("volume", DoubleType(),  True)
    ])

    # ─────────────────────────────────────────────────────────────────────
    # Đọc luồng dữ liệu từ Kafka topic "crypto_trades"
    # startingOffsets = "earliest" để đảm bảo không bỏ sót dữ liệu cũ
    # ─────────────────────────────────────────────────────────────────────
    logger.info("Reading stream from Kafka topic 'crypto_trades'...")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "crypto_trades") \
        .option("startingOffsets", "earliest") \
        .load()

    # ─────────────────────────────────────────────────────────────────────
    # Parse JSON → Ép kiểu → Chuyển cột 'time' thành Timestamp
    # Đây là bước Bronze Layer (dữ liệu thô có cấu trúc)
    # ─────────────────────────────────────────────────────────────────────
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("time", to_timestamp(col("time")))

    # ─────────────────────────────────────────────────────────────────────
    # ÁP DỤNG PIPELINE LÀM SẠCH 7 BƯỚC (Bronze → Silver Layer)
    # ─────────────────────────────────────────────────────────────────────
    logger.info("Applying 7-step Data Cleaning Pipeline (Bronze -> Silver)...")
    cleaned_df = clean_data(parsed_df)

    # ─────────────────────────────────────────────────────────────────────
    # Ghi dữ liệu sạch xuống Hive (HDFS Parquet) dưới dạng streaming
    # Checkpoint đảm bảo exactly-once: không mất, không trùng dữ liệu
    # ─────────────────────────────────────────────────────────────────────
    logger.info("Writing cleaned stream to Hive table 'crypto_trades'...")

    query = cleaned_df.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoint_crypto_final") \
        .toTable("crypto_trades")

    try:
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Spark streaming pipeline terminated exceptionally: {e}")


if __name__ == "__main__":
    main()
