from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DropTable") \
    .config("hive.metastore.uris", "thrift://master:9083") \
    .config("spark.sql.hive.metastore.version", "4.0.0") \
    .config("spark.sql.hive.metastore.jars", "path") \
    .config("spark.sql.hive.metastore.jars.path", "file:///opt/hive/lib/*") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("DROP TABLE IF EXISTS crypto_trades")
print("TABLE DROPPED SUCCESSFULLY!")
