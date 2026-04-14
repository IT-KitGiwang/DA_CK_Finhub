"""
Kafka Producer for Finnhub Real-time Trades
Connects to Finnhub WebSocket API to scrape real-time market data
and publishes the formatted payload directly to an Apache Kafka branch.
"""

import websocket
import json
import os
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Any, List

# Configure application logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [PRODUCER] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = ".env") -> None:
    """Loads environment variables securely from .env if present."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())

class FinnhubCryptoProducer:
    """Class to manage Finnhub WebSocket connection and Kafka Publishing."""
    
    def __init__(self):
        self.api_key: str = os.getenv("FINNHUB_API_KEY", "")
        if not self.api_key:
            raise ValueError("Configuration Missing: FINNHUB_API_KEY in .env")

        self.kafka_broker: str = os.getenv("KAFKA_BROKER", "localhost:9094")
        self.kafka_topic: str = os.getenv("KAFKA_TOPIC", "crypto_trades")
        
        raw_symbols = os.getenv("SYMBOLS", "BINANCE:BTCUSDT,BINANCE:ETHUSDT,BINANCE:BNBUSDT")
        self.symbols: List[str] = [sym.strip() for sym in raw_symbols.split(",") if sym.strip()]
        
        self.scrape_interval: float = float(os.getenv("SCRAPE_INTERVAL", "1.0"))
        self.last_sent_times: Dict[str, float] = {}

        logger.info(f"Connecting to Kafka Broker sequence: {self.kafka_broker}...")
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.ws = None

    def on_message(self, ws, message: str) -> None:
        """Callback triggered when a WebSocket message is received."""
        data: Dict[str, Any] = json.loads(message)

        if data.get("type") == "trade":
            for trade in data.get("data", []):
                symbol: str = trade.get("s")
                
                # Rate Limiting: Respect SCRAPE_INTERVAL per symbol
                current_time = time.time()
                if symbol in self.last_sent_times and current_time - self.last_sent_times[symbol] < self.scrape_interval:
                    continue
                
                self.last_sent_times[symbol] = current_time
                
                price: float = trade.get("p")
                volume: float = trade.get("v")
                timestamp: int = trade.get("t")

                # Parse and render standardized timestamp format
                time_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

                payload = {
                    "time": time_str,
                    "symbol": symbol,
                    "price": price,
                    "volume": volume
                }

                # Push dynamically to Kafka Stream
                self.producer.send(self.kafka_topic, value=payload)
                logger.info(f"[KAFKA SENT] - {time_str} | {symbol} | Price: {price} | Vol: {volume}")

    def on_error(self, ws, error: Exception) -> None:
        """Callback for network or parsing errors."""
        logger.error(f"WebSocket Error Received: {error}")

    def on_close(self, ws, close_status_code, close_msg) -> None:
        """Callback on WebSocket closure."""
        logger.warning(f"WebSocket Closed. Code: {close_status_code}, Message: {close_msg}")
        self.producer.close()

    def on_open(self, ws) -> None:
        """Callback explicitly tracking the active socket connection start."""
        logger.info("Successfully connected to Finnhub WebSocket API!")
        
        # Dispatch subscription commands
        for symbol in self.symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            logger.info(f"Subscribed explicitly to sequence: {symbol}")

    def start(self) -> None:
        """Starts the WebSocket event loop."""
        socket_url = f"wss://ws.finnhub.io?token={self.api_key}"
        self.ws = websocket.WebSocketApp(
            socket_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        logger.info("Initiating Producer Execution Loop. Press Ctrl+C to terminate.")
        try:
            self.ws.run_forever()
        except KeyboardInterrupt:
            self.producer.close()
            logger.info("User requested shutdown. Producer successfully terminated.")

def main() -> None:
    # Set up runtime configuration and dispatch
    load_dotenv_file()
    producer = FinnhubCryptoProducer()
    producer.start()

if __name__ == "__main__":
    main()
