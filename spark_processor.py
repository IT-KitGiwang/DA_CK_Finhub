from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Cấu hình Spark Session tương thích Hive 4
spark = SparkSession.builder \
    .appName("CryptoKafkaToHive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://master:9083") \
    .config("spark.sql.hive.metastore.version", "3.1.2") \
    .config("spark.sql.hive.metastore.jars", "builtin") \
    .enableHiveSupport() \
    .getOrCreate()

schema = StructType([
    StructField("time", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_trades") \
    .load()

# Chuyển đổi 'time' từ String sang Timestamp để Superset vẽ được Chart thời gian
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("time", to_timestamp(col("time")))

query = parsed_df.writeStream \
    .outputMode("append") \
    .format("hive") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_crypto_v2") \
    .toTable("crypto_trades")

query.awaitTermination()
