"""
Kafka Consumer Utility
A diagnostic script to consume and debug real-time data points from the Kafka topic.
"""

import json
import logging
import os
from typing import Dict, Any
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = ".env") -> None:
    """Loads environment variables from a local .env file."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())

def main() -> None:
    # Initialize environment configuration
    load_dotenv_file()

    # Kafka cluster properties
    kafka_broker: str = os.getenv("KAFKA_BROKER", "localhost:9094")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "crypto_trades")

    logger.info(f"Awaiting streaming data from Kafka Broker at {kafka_broker}...")
    
    try:
        # Initialize Kafka consumer
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=[kafka_broker],
            auto_offset_reset='earliest',  # Always consume from the beginning of the topic if no history
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Continuously monitor messages
        for message in consumer:
            data: Dict[str, Any] = message.value
            
            # Verify data schema formatting
            is_valid_payload = isinstance(data, dict) and all(key in data for key in ['time', 'symbol', 'price', 'volume'])
            
            if is_valid_payload:
                logger.info(f"📦 [XUẤT TỪ KAFKA] -> Thời gian: {data['time']}, Đồng coin: {data['symbol']}, "
                            f"Giá: {data['price']}, Khối lượng: {data['volume']}")
            else:
                logger.warning(f"⚠️ [DỮ LIỆU KHÁC/CŨ] -> {data}")
                
    except Exception as e:
        logger.error(f"Error during Kafka consumption: {e}")

if __name__ == "__main__":
    main()
