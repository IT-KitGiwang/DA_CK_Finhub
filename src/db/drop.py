"""
Drop Table Script
A utility database module targeting Hive metadata components
to completely drop the crypto_trades table programmatically.
"""

from pyspark.sql import SparkSession
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="[HIVE ACTION] %(message)s")
logger = logging.getLogger(__name__)

def main() -> None:
    logger.info("Initializing SparkSession to communicate with Hive Metastore...")
    
    spark = SparkSession.builder \
        .appName("DropTable") \
        .config("hive.metastore.uris", "thrift://master:9083") \
        .config("spark.sql.hive.metastore.version", "4.0.0") \
        .config("spark.sql.hive.metastore.jars", "path") \
        .config("spark.sql.hive.metastore.jars.path", "file:///opt/hive/lib/*") \
        .enableHiveSupport() \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # Perform the drop operation
    logger.info("Issuing DROP TABLE command for 'crypto_trades'...")
    spark.sql("DROP TABLE IF EXISTS crypto_trades")
    
    logger.info("TABLE DROPPED SUCCESSFULLY!")

if __name__ == "__main__":
    main()
