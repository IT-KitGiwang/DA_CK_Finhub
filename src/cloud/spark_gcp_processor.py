"""
Module: Hybrid Spark Streaming Processor (Local to GCP)
Mô tả: 
 - Lắng nghe dòng dữ liệu từ cụm Kafka cục bộ (Local Docker).
 - Xử lý, ép kiểu dữ liệu (Schema) và trích xuất thời gian.
 - Đẩy dữ liệu (Stream) dọc theo 2 đường tới Google Cloud Platform:
   1. Google Cloud Storage (GCS): Lưu trữ dài hạn (Data Lake) dưới dạng file Parquet.
   2. Google BigQuery: Kho dữ liệu truy vấn tốc độ cao (Data Warehouse) cho Dashboard.
"""

import os
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.utils import AnalysisException

# 1. Cấu hình Logging đầy đủ theo luật Rule Code
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [SPARK-GCP-HYBRID] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = ".env") -> None:
    """
    Hàm đọc cấu hình từ file .env.
    - Cố gắng tìm file .env, bỏ qua các dòng comment hoặc rỗng.
    - Được thiết kế try-catch đầy đủ để tránh chết app nếu thiếu file.
    """
    try:
        if not os.path.exists(path):
            logger.warning(f"Không tìm thấy file {path}. Sẽ sử dụng System Env trực tiếp.")
            return
        
        with open(path, "r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                # Bỏ qua dòng trống hoặc dòng chú thích (bắt đầu bằng #)
                if not line or line.startswith("#") or "=" not in line: 
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())
        logger.info("Đã nạp file .env thành công.")
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng khi đọc file .env: {e}")
        # Không crack app, vẫn ráng chạy tiếp với hy vọng môi trường đã set sẵn

def main() -> None:
    """Hàm chính khởi chạy quy trình Spark Streaming"""
    # Bước 1: Nạp file .env
    load_dotenv_file()

    try:
        # Bước 2: Lấy các cấu hình từ System Environment (đã nạp từ .env)
        logger.info("Bắt đầu trích xuất các khoá cấu hình GCP từ biến môi trường...")
        
        kafka_broker = os.getenv("KAFKA_BROKER")
        kafka_topic = os.getenv("KAFKA_TOPIC")
        gcp_project = os.getenv("GCP_PROJECT_ID")
        gcs_bucket = os.getenv("GCP_GCS_BUCKET")
        bq_dataset = os.getenv("GCP_BQ_DATASET")
        bq_table_name = os.getenv("GCP_BQ_TABLE_STREAM")
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        
        # Kiểm tra tính toàn vẹn của config
        if not all([kafka_broker, kafka_topic, gcp_project, gcs_bucket, bq_dataset, bq_table_name, credentials_path]):
            raise ValueError("Thiếu biến môi trường quan trọng, tiến trình dừng lại.")
            
        bq_table = f"{gcp_project}.{bq_dataset}.{bq_table_name}"
        
        # Bước 3: Khởi tạo Spark Session với Connectors Cloud của Google
        logger.info("Đang khởi tạo Spark Session với cấu hình BigQuery & GCS Native Connectors...")
        spark = SparkSession.builder \
            .appName("Hybrid_Kafka_to_GCP_Pipeline") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_path) \
            .config("viewsEnabled", "true") \
            .config("materializationDataset", bq_dataset) \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Khởi tạo Spark Session THÀNH CÔNG.")

        # Bước 4: Định nghĩa Schema khớp với gói dữ liệu Finnhub
        schema = StructType([
            StructField("time", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", DoubleType(), True)
        ])

        # Bước 5: Kết nối lắng nghe dòng chảy Kafka cục bộ
        logger.info(f"Đang kết nối để lắng nghe Kafka tại Broker: {kafka_broker} | Topic: {kafka_topic}")
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # Tiền xử lý dữ liệu: Tách mảng JSON, ép kiểu thời gian và sinh Partitions
        parsed_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", to_timestamp(col("time"))) \
            .withColumn("year", year(col("timestamp"))) \
            .withColumn("month", month(col("timestamp"))) \
            .withColumn("day", dayofmonth(col("timestamp")))
            
        logger.info("Đã thiết lập xong luồng chuyển đổi Transformation.")

        # Bước 6: Mở luồng Output Sink thứ 1 đẩy lên GCS (Data Lake)
        logger.info(f"Kích hoạt nhánh ghi dữ liệu Parquet lên Google Cloud Storage (GCS): gs://{gcs_bucket}/clean_data/")
        gcs_query = parsed_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", f"gs://{gcs_bucket}/clean_data/") \
            .option("checkpointLocation", "/tmp/spark_gcs_checkpoint") \
            .partitionBy("year", "month", "day") \
            .trigger(processingTime="15 seconds") \
            .start()

        # Bước 7: Mở luồng Output Sink thứ 2 đẩy lên BigQuery (Data Warehouse)
        logger.info(f"Kích hoạt nhánh ghi dữ liệu lên BigQuery Table: {bq_table}")
        bq_query = parsed_df.writeStream \
            .format("bigquery") \
            .option("table", bq_table) \
            .option("checkpointLocation", "/tmp/spark_bq_checkpoint") \
            .option("temporaryGcsBucket", gcs_bucket) \
            .outputMode("append") \
            .trigger(processingTime="15 seconds") \
            .start()

        # Bước 8: Neo chờ tiến trình kết thúc (Chạy vĩnh viễn unless Stopped)
        logger.info("🔥 Bắt đầu vận chuyển dữ liệu lên GCP - Các luồng (Streams) đang hoạt động...")
        spark.streams.awaitAnyTermination()

    except AnalysisException as ae:
        logger.error(f"Lỗi cú pháp phân tích Spark SQL / Schema: {ae}")
    except ValueError as ve:
        logger.error(f"Lỗi khởi tạo hoặc thiếu tham số cấu hình: {ve}")
    except Exception as e:
        logger.critical(f"Lỗi ngoại lệ nghiêm trọng làm crash tiến trình Spark: {e}", exc_info=True)
    finally:
        logger.info("Kết thúc phiên chạy Spark Application (Shutting down).")

if __name__ == "__main__":
    main()
