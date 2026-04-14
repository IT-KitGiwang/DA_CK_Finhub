"""
Spark Streaming Processor with Data Cleaning Pipeline
===================================================
Pipeline: Finnhub WebSocket → Kafka → [SPARK CLEANING] → Hive (HDFS / Parquet)
Layer:    Bronze (Raw Kafka) → Silver (Cleaned) → Hive table (crypto_trades)
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, trim, upper,
    current_timestamp, unix_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Constants and Configurations
VALID_SYMBOLS = [
    "BINANCE:BTCUSDT",
    "BINANCE:ETHUSDT",
    "BINANCE:BNBUSDT"
]

PRICE_BOUNDS = {
    "BINANCE:BTCUSDT": {"min": 1000,   "max": 500000},
    "BINANCE:ETHUSDT": {"min": 50,     "max": 50000},
    "BINANCE:BNBUSDT": {"min": 10,     "max": 5000},
}

MAX_VOLUME = {
    "BINANCE:BTCUSDT": 1000,
    "BINANCE:ETHUSDT": 50000,
    "BINANCE:BNBUSDT": 100000,
}

MAX_FUTURE_SECONDS = 86400      # 24 hours
MAX_PAST_SECONDS   = 2592000    # 30 days


def clean_data(raw_df: DataFrame) -> DataFrame:
    """
    Applies a 7-step data cleaning pipeline to the raw DataFrame.
    """
    # 1. Null Elimination: Drop rows with essential missing values
    step1_df = raw_df.dropna(how="any", subset=["time", "symbol", "price", "volume"])

    # 2. String Normalization: Trim and uppercase symbols
    step2_df = step1_df.withColumn("symbol", upper(trim(col("symbol"))))

    # 3. Symbol Whitelist: Keep only registered symbols
    step3_df = step2_df.filter(col("symbol").isin(VALID_SYMBOLS))

    # 4. Price Validation: Ensure price is within realistic bounds
    price_condition = lit(False)
    for symbol, bounds in PRICE_BOUNDS.items():
        price_condition |= (
            (col("symbol") == symbol) &
            (col("price") >= bounds["min"]) &
            (col("price") <= bounds["max"])
        )
    step4_df = step3_df.filter(price_condition)

    # 5. Volume Validation: Ensure strictly positive volume within typical limits
    volume_condition = lit(False)
    for symbol, max_vol in MAX_VOLUME.items():
        volume_condition |= (
            (col("symbol") == symbol) &
            (col("volume") > 0) &
            (col("volume") <= max_vol)
        )
    step5_df = step4_df.filter(volume_condition)

    # 6. Timestamp Validation: Avoid extreme future or past timestamps
    current_time = unix_timestamp(current_timestamp())
    record_time = unix_timestamp(col("time"))
    step6_df = step5_df.filter(
        (record_time <= current_time + MAX_FUTURE_SECONDS) &
        (record_time >= current_time - MAX_PAST_SECONDS)
    )

    # 7. Deduplication: Remove redundant messages in the micro-batch
    step7_df = step6_df.dropDuplicates(["time", "symbol", "price", "volume"])

    logger.info("Data Cleaning Pipeline completed — 7/7 steps applied successfully.")
    return step7_df


def main() -> None:
    logger.info("Initializing Spark Session for Hive & Kafka streaming...")

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

    # Fixed schema mapping Finnhub structured payload
    schema = StructType([
        StructField("time",   StringType(),  True),
        StructField("symbol", StringType(),  True),
        StructField("price",  DoubleType(),  True),
        StructField("volume", DoubleType(),  True)
    ])

    logger.info("Reading stream from Kafka topic 'crypto_trades'...")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "crypto_trades") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON from Kafka value -> string -> JSON object
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("time", to_timestamp(col("time")))

    logger.info("Applying 7-step Data Cleaning Pipeline (Bronze -> Silver)...")
    cleaned_df = clean_data(parsed_df)

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
