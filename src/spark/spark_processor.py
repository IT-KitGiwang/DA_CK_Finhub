"""
Spark Streaming Processor
Consumes real-time cryptocurrency data from Kafka, processes and formats it,
and writes the data into Apache Hive (HDFS) as a streaming pipeline.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [SPARK] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def main() -> None:
    logger.info("Initializing Spark Session for Hive & Kafka streaming...")
    
    # Configure Spark Session compatible with Hive 4.0.0
    spark = SparkSession.builder \
        .appName("CryptoKafkaToHive") \
        .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://master:9083") \
        .config("spark.sql.hive.metastore.version", "4.0.0") \
        .config("spark.sql.hive.metastore.jars", "path") \
        .config("spark.sql.hive.metastore.jars.path", "file:///opt/hive/lib/*") \
        .enableHiveSupport() \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Define schema matching Finnhub payload structure
    schema = StructType([
        StructField("time", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True)
    ])

    logger.info("Reading stream from Kafka topic 'crypto_trades'...")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "crypto_trades") \
        .option("startingOffsets", "earliest") \
        .load()

    # Cast JSON value, unpack it, and convert 'time' field to Timestamp type for Superset queries
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("time", to_timestamp(col("time")))

    logger.info("Writing stream to Hive table 'crypto_trades'...")
    
    # Write streaming micro-batches out to Hive
    query = parsed_df.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoint_crypto_final") \
        .toTable("crypto_trades")

    try:
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Spark streaming pipeline terminated exceptionally: {e}")

if __name__ == "__main__":
    main()
