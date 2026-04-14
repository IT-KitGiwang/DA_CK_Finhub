"""
Hybrid Spark Streaming Processor (Local to GCP)
Reads real-time cryptocurrency data from Local Docker Kafka,
Processes and extracts standard schema, then writes stream to:
1. Google Cloud Storage (GCS) - As Data Lake (Parquet format)
2. Google BigQuery - As Data Warehouse (Enterprise Analytics)
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Configure application logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [SPARK-GCP-HYBRID] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = "../../.env") -> None:
    # Safely load environment if executed from IDE or locally
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())

def main() -> None:
    load_dotenv_file()

    # Extract GCP Configurations
    kafka_broker = os.getenv("KAFKA_BROKER")
    kafka_topic = os.getenv("KAFKA_TOPIC")
    
    gcp_project = os.getenv("GCP_PROJECT_ID")
    gcs_bucket = os.getenv("GCP_GCS_BUCKET")
    bq_dataset = os.getenv("GCP_BQ_DATASET")
    bq_table = f"{gcp_project}.{bq_dataset}.{os.getenv('GCP_BQ_TABLE_STREAM')}"

    logger.info("Organizing Spark Session with BigQuery & GCS Native Connectors...")
    
    # Needs Packages: 
    # --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2
    # --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.14
    spark = SparkSession.builder \
        .appName("Hybrid_Kafka_to_GCP_Pipeline") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) \
        .config("viewsEnabled", "true") \
        .config("materializationDataset", bq_dataset) \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Mapping Finnhub structured schema
    schema = StructType([
        StructField("time", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True)
    ])

    logger.info(f"Subscribing to Stream Local Kafka from {kafka_broker} | Topic: {kafka_topic}")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("time"))) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp")))

    # -------------------------------------------------------------
    # SINK 1: Google Cloud Storage (GCS) - Data Lake Archive
    # Partitioned by year, month, day for efficient big-data scanning
    # -------------------------------------------------------------
    logger.info(f"Opening parallel write stream to GCS: gs://{gcs_bucket}/crypto_lake/")
    gcs_query = parsed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"gs://{gcs_bucket}/crypto_lake/") \
        .option("checkpointLocation", "/tmp/spark_gcs_checkpoint") \
        .partitionBy("year", "month", "day") \
        .start()

    # -------------------------------------------------------------
    # SINK 2: Google BigQuery - Enterprise Data Warehouse
    # Push records incrementally to BigQuery for real-time dashboards
    # -------------------------------------------------------------
    logger.info(f"Opening parallel write stream to BigQuery Table: {bq_table}")
    bq_query = parsed_df.writeStream \
        .format("bigquery") \
        .option("table", bq_table) \
        .option("checkpointLocation", "/tmp/spark_bq_checkpoint") \
        .option("temporaryGcsBucket", gcs_bucket) \
        .outputMode("append") \
        .start()

    try:
        # Await streams termination concurrently
        spark.streams.awaitAnyTermination()
    except Exception as e:
        logger.error(f"GCP Hybrid Spark Pipeline encountered an error: {e}")

if __name__ == "__main__":
    main()
