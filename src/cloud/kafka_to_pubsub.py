"""
Hybrid Cloud Pub/Sub Bridge
Reads real-time messages directly from Local Kafka Broker and
pushes them into Google Cloud Pub/Sub.
This allows GCP Serverless compute engines (like Dataflow or Cloud Functions)
to tap into the local data stream asynchronously.
"""

import os
import json
import logging
from kafka import KafkaConsumer
from google.cloud import pubsub_v1
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PUBSUB-BRIDGE] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = "../../.env") -> None:
    if not os.path.exists(path): return
    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())

class KafkaToPubSubBridge:
    def __init__(self):
        # Local Local Kafka Configure
        self.kafka_broker = os.getenv("KAFKA_BROKER")
        self.kafka_topic = os.getenv("KAFKA_TOPIC")

        # GCP Pub/Sub Configure
        # Must have GOOGLE_APPLICATION_CREDENTIALS set in env
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.pubsub_topic_id = os.getenv("GCP_PUBSUB_TOPIC")
        
        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is missing for Pub/Sub")

        # Initialize Kafka Consumer
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=[self.kafka_broker],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='gcp-pubsub-bridge-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Initialize Pub/Sub Publisher
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.pubsub_topic_id)
        
        # Ensure Topic Exists or Log a warning
        logger.info(f"Initializing Bridge: [{self.kafka_topic}] (Local) -> [{self.topic_path}] (Cloud)")

    def run(self):
        logger.info("Starting local Kafka -> GCP Pub/Sub bridge. Waiting for messages...")
        try:
            for message in self.consumer:
                data = message.value
                symbol = data.get("symbol", "UNKNOWN")
                
                # Payload encoded to bytes 
                data_bytes = json.dumps(data).encode("utf-8")
                
                # Push asynchronous flow to Pub/sub
                # Add attributes to Pub/Sub to allow serverless routing rules and filtering 
                future = self.publisher.publish(
                    self.topic_path, 
                    data_bytes, 
                    symbol=symbol,
                    origin="local-docker-bridge"
                )
                
                # Get the message_id returned by Pub/Sub
                message_id = future.result()
                logger.info(f"Bridged to Pub/Sub: {symbol} - Message ID {message_id}")

        except KeyboardInterrupt:
            logger.info("Kafka to Pub/Sub Bridge stopped safely by user.")
        except Exception as e:
            logger.error(f"Encountered error spanning bridge: {e}")
            raise
        finally:
            self.consumer.close()

def main():
    load_dotenv_file()
    bridge = KafkaToPubSubBridge()
    bridge.run()

if __name__ == "__main__":
    main()
