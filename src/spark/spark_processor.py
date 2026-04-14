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
    "BINANCE:BNBUSDT"
]

MAX_FUTURE_SECONDS = 86400      # 24 giờ
MAX_PAST_SECONDS   = 2592000    # 30 ngày


def clean_data(raw_df: DataFrame) -> DataFrame:
    """
    Áp dụng quy trình kỹ thuật làm sạch và làm giàu dữ liệu lên DataFrame thô.
    """
    # 1. Loại bỏ Null: Xóa các dòng bị khuyết các trường quan trọng (Sự cố bắt packet)
    step1_df = raw_df.dropna(how="any", subset=["time", "symbol", "price", "volume"])

    # 2. Chuẩn hóa chuỗi: Cắt bỏ khoảng trắng dư thừa và in hoa tên token nhằm tránh lỗi định dạng
    step2_df = step1_df.withColumn("symbol", upper(trim(col("symbol"))))

    # 3. Lọc danh sách trắng (Whitelist): Chỉ xử lý các coin có trong danh mục đang theo dõi
    step3_df = step2_df.filter(col("symbol").isin(VALID_SYMBOLS))

    # 4. Xác thực Hợp lý Về Mặt Toán Học (Không hardcode ngưỡng):
    # Dù giá chạy theo thị trường, nhưng price và volume luôn phi vật lý nếu nhỏ hơn hoặc bằng 0
    step4_df = step3_df.filter((col("price") > 0) & (col("volume") > 0))

    # 5. Xác thực Thời Gian (Timestamp): Khử các gói tin từ tương lai quá xa hoặc quá khứ xa do lệch kim đồng hồ (Clock Drift)
    current_time = unix_timestamp(current_timestamp())
    record_time = unix_timestamp(col("time"))
    step5_df = step4_df.filter(
        (record_time <= current_time + MAX_FUTURE_SECONDS) &
        (record_time >= current_time - MAX_PAST_SECONDS)
    )

    # 6. Khử trùng lặp (Deduplication): Loại bỏ bản ghi có thể bị gởi đúp từ cơ chế at-least-once của Kafka
    step6_df = step5_df.dropDuplicates(["time", "symbol", "price", "volume"])

    logger.info("Hoàn tất tiến trình xử lý Data Cleaning hợp lý (6 bước bảo vệ cốt lõi).")
    return step6_df


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
