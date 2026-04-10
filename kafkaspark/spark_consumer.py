from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("CryptoStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Định nghĩa khuôn mẫu dữ liệu
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

# Đọc từ Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crawled_data").load()

# Giải mã JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# Ghi vào HDFS mỗi 5 giây
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .trigger(processingTime='5 seconds') \
    .option("path", "hdfs://master:9000/user/dack15/data_lake/") \
    .option("checkpointLocation", "hdfs://master:9000/user/dack15/checkpoints/") \
    .start()

print("🔥 Spark đang đợi dữ liệu (5s/lần)...")
query.awaitTermination()