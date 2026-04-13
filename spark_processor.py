from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Khởi tạo Spark Session với hỗ trợ Hive
spark = SparkSession.builder \
    .appName("CryptoKafkaToHive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://master:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Định nghĩa Schema cho dữ liệu JSON từ Kafka
schema = StructType([
    StructField("time", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

# 3. Đọc dữ liệu từ Kafka Topic
# Lưu ý: kafka.bootstrap.servers lấy từ Docker network "kafka:9092"
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_trades") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse dữ liệu JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Ghi dữ liệu vào bảng Hive (Parquet)
# Sử dụng append mode để thêm dữ liệu liên tục
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("hive") \
    .option("path", "/user/hive/warehouse/crypto_trades") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_crypto") \
    .toTable("crypto_trades")

query.awaitTermination()
