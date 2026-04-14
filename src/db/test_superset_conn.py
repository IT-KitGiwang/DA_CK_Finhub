"""
Hive Thrift Test Connection
Direct verification script to execute query bindings and ensure HiveServer2
thrift protocols successfully respond over Port 10000.
"""

from pyhive import hive
import logging

# Configure standardized root logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [HIVE_TEST] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def evaluate_connection() -> None:
    logger.info("Attempting to securely bind NOSASL context to HiveServer2 on master:10000...")
    try:
        # Establish Hive Session
        conn = hive.connect(host="master", port=10000, username="dack15", database="default", auth="NOSASL")
        cursor = conn.cursor()
        
        logger.info("Session established. Requesting top sequential block from 'crypto_trades'...")
        cursor.execute("SELECT * FROM crypto_trades LIMIT 3")
        
        rows = cursor.fetchall()
        for idx, row in enumerate(rows):
            logger.info(f"Row {idx+1}: {row}")
            
        logger.info(f"SUCCESS! Fetched {len(rows)} block allocations explicitly.")
        
    except Exception as e:
        logger.error(f"Unable to process thrift configuration request: {e}")
        logger.error("Please verify that Hive service infrastructure is running actively.")

if __name__ == "__main__":
    evaluate_connection()
