"""
Trình Xử Lý Dữ Liệu Thời Gian Thực Bằng Spark Streaming
===================================================
Luồng dữ liệu (Pipeline): Finnhub WebSocket → Kafka → [SPARK LÀM SẠCH] → Hive (HDFS / Parquet)
Phân lớp (Layer):    Lớp Đồng (Raw Kafka) → Lớp Bạc (Cleaned) → Bảng Hive (crypto_trades)
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, trim, upper,
    current_timestamp, unix_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Cấu hình mức độ Log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Các Hằng Số và Cấu Hình Cơ Bản
VALID_SYMBOLS = [
    "BINANCE:BTCUSDT",
    "BINANCE:ETHUSDT",
    "BINANCE:BNBUSDT",
    "BINANCE:SOLUSDT",
    "BINANCE:DOGEUSDT"
]

PRICE_BOUNDS = {
    "BINANCE:BTCUSDT": {"min": 1000,   "max": 500000},
    "BINANCE:ETHUSDT": {"min": 50,     "max": 50000},
    "BINANCE:BNBUSDT": {"min": 10,     "max": 5000},
    "BINANCE:SOLUSDT": {"min": 1,      "max": 1000},
    "BINANCE:DOGEUSDT":{"min": 0.001,  "max": 10},
}

MAX_VOLUME = {
    "BINANCE:BTCUSDT": 1000,
    "BINANCE:ETHUSDT": 50000,
    "BINANCE:BNBUSDT": 100000,
    "BINANCE:SOLUSDT": 500000,
    "BINANCE:DOGEUSDT": 10000000,
}

MAX_FUTURE_SECONDS = 86400      # 24 giờ
MAX_PAST_SECONDS   = 2592000    # 30 ngày


def clean_data(raw_df: DataFrame) -> DataFrame:
    """
    Áp dụng quy trình kỹ thuật làm sạch dữ liệu 7 bước lên DataFrame thô.
    """
    # 1. Loại bỏ Null: Xóa các dòng bị khuyết các trường quan trọng
    step1_df = raw_df.dropna(how="any", subset=["time", "symbol", "price", "volume"])

    # 2. Chuẩn hóa chuỗi: Cắt bỏ khoảng trắng dư thừa và in hoa (Uppercase) tên đồng token
    step2_df = step1_df.withColumn("symbol", upper(trim(col("symbol"))))

    # 3. Lọc danh sách trắng (Whitelist): Chỉ giữ lại các mã Token đã được cấp phép
    step3_df = step2_df.filter(col("symbol").isin(VALID_SYMBOLS))

    # 4. Xác thực Giá (Price): Đảm bảo giá trị nằm trong ngưỡng hợp lý (loại trừ giá trị rác)
    price_condition = lit(False)
    for symbol, bounds in PRICE_BOUNDS.items():
        price_condition |= (
            (col("symbol") == symbol) &
            (col("price") >= bounds["min"]) &
            (col("price") <= bounds["max"])
        )
    step4_df = step3_df.filter(price_condition)

    # 5. Xác thực Khối Lượng (Volume): Phải dương và không vượt quá giới hạn cực đoan
    volume_condition = lit(False)
    for symbol, max_vol in MAX_VOLUME.items():
        volume_condition |= (
            (col("symbol") == symbol) &
            (col("volume") > 0) &
            (col("volume") <= max_vol)
        )
    step5_df = step4_df.filter(volume_condition)

    # 6. Xác thực Thời Gian (Timestamp): Khử các gói tin từ tương lai quá xa hoặc quá khứ xa
    current_time = unix_timestamp(current_timestamp())
    record_time = unix_timestamp(col("time"))
    step6_df = step5_df.filter(
        (record_time <= current_time + MAX_FUTURE_SECONDS) &
        (record_time >= current_time - MAX_PAST_SECONDS)
    )

    # 7. Khử trùng lặp (Deduplication): Loại bỏ bản ghi lặp lại trong cùng Micro-batch
    step7_df = step6_df.dropDuplicates(["time", "symbol", "price", "volume"])

    logger.info("Hoàn tất tiến trình xử lý Data Cleaning — Thực thi thành công toàn bộ 7/7 bước.")
    return step7_df


def main() -> None:
    logger.info("Đang khởi tạo Spark Session cho Hive & Kafka streaming...")

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

    # Bản đồ Schema ánh xạ tương thích với JSON được gửi từ Finnhub
    schema = StructType([
        StructField("time",   StringType(),  True),
        StructField("symbol", StringType(),  True),
        StructField("price",  DoubleType(),  True),
        StructField("volume", DoubleType(),  True)
    ])

    logger.info("Đang đọc luồng Streaming từ topic 'crypto_trades' trong cụm Kafka...")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "crypto_trades") \
        .option("startingOffsets", "earliest") \
        .load()

    # Dịch nguyên bản luồng JSON tải trọng (Payload) từ định nghĩa Chuỗi text sang cấu trúc JSON Struct
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("time", to_timestamp(col("time")))

    logger.info("Thực thi cơ chế làm sạch Dữ Liệu 7 Bước Đạt Chuẩn (Chuyển Hóa Bronze -> Silver)...")
    cleaned_df = clean_data(parsed_df)

    logger.info("Bắt đầu ghi phân mảnh luồng dữ liệu sạch trực tiếp tới Hệ Thống Hive 'crypto_trades'...")
    query = cleaned_df.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoint_crypto_final") \
        .toTable("crypto_trades")

    try:
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Khối lượng công việc Spark Streaming gián đoạn đột ngột: {e}")


if __name__ == "__main__":
    main()
