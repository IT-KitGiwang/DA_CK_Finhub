"""
Kafka Producer cho Dữ liệu Tài sản số Thời gian thực (Real-time Crypto Trades)
Kết nối với Finnhub WebSocket API để thu thập dữ liệu giao dịch thị trường theo thời gian thực
và đẩy gói dữ liệu đã được định dạng trực tiếp vào hệ thống Apache Kafka.
"""

import websocket
import json
import os
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Any, List

# Cấu hình logging cho ứng dụng
logging.basicConfig(level=logging.INFO, format="%(asctime)s [PRODUCER] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = ".env") -> None:
    """Đọc các biến môi trường một cách bảo mật từ file .env nếu có."""
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
    """Lớp quản lý kết nối Finnhub WebSocket chạy ngầm và Đẩy dữ liệu vào Kafka."""
    
    def __init__(self):
        self.api_key: str = os.getenv("FINNHUB_API_KEY", "")
        if not self.api_key:
            raise ValueError("Thiếu cấu hình: Cần có FINNHUB_API_KEY trong file .env")

        self.kafka_broker: str = os.getenv("KAFKA_BROKER", "localhost:9094")
        self.kafka_topic: str = os.getenv("KAFKA_TOPIC", "crypto_trades")
        
        raw_symbols = os.getenv("SYMBOLS", "BINANCE:BTCUSDT,BINANCE:ETHUSDT,BINANCE:BNBUSDT,BINANCE:SOLUSDT")
        self.symbols: List[str] = [sym.strip() for sym in raw_symbols.split(",") if sym.strip()]
        
        self.scrape_interval: float = float(os.getenv("SCRAPE_INTERVAL", "1.0"))
        self.last_sent_times: Dict[str, float] = {}

        logger.info(f"Đang kết nối đến Kafka Broker tại: {self.kafka_broker}...")
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.ws = None

    def on_message(self, ws, message: str) -> None:
        """Hàm kích hoạt (Callback) mỗi khi có dòng dữ liệu mới đổ về từ WebSocket."""
        data: Dict[str, Any] = json.loads(message)

        if data.get("type") == "trade":
            for trade in data.get("data", []):
                symbol: str = trade.get("s")
                
                # Giới hạn tốc độ lấy dữ liệu (Rate Limiting) dựa trên SCRAPE_INTERVAL
                current_time = time.time()
                if symbol in self.last_sent_times and current_time - self.last_sent_times[symbol] < self.scrape_interval:
                    continue
                
                self.last_sent_times[symbol] = current_time
                
                price: float = trade.get("p")
                volume: float = trade.get("v")
                timestamp: int = trade.get("t")

                # Parse và định dạng lại mốc thời gian (timestamp) theo chuẩn
                time_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

                payload = {
                    "time": time_str,
                    "symbol": symbol,
                    "price": price,
                    "volume": volume
                }

                # Đẩy luồng dữ liệu (Push) động lên hệ thống Kafka
                self.producer.send(self.kafka_topic, value=payload)
                logger.info(f"[KAFKA SENT] - {time_str} | {symbol} | Price: {price} | Vol: {volume}")

    def on_error(self, ws, error: Exception) -> None:
        """Hàm ghi nhận lỗi khi mạng rớt hoặc phân tách dữ liệu lỗi."""
        logger.error(f"Đã bắt gặp lỗi từ WebSocket: {error}")

    def on_close(self, ws, close_status_code, close_msg) -> None:
        """Hàm kích hoạt khi WebSocket bị ngắt."""
        logger.warning(f"WebSocket đã đóng. Mã đóng (Code): {close_status_code}, Tin nhắn (Message): {close_msg}")
        self.producer.close()

    def on_open(self, ws) -> None:
        """Hàm báo cáo chính thức kết nối hoàn tất."""
        logger.info("Kết nối thành công tới Finnhub WebSocket API!")
        
        # Gửi lệnh đăng ký lắng nghe (Subscribe) cho các đồng Token được quy định
        for symbol in self.symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            logger.info(f"Đã gửi lệnh Subscribe theo dõi: {symbol}")

    def start(self) -> None:
        """Hàm khởi động vòng lặp sự kiện bất tận (Event Loop) của WebSocket."""
        socket_url = f"wss://ws.finnhub.io?token={self.api_key}"
        self.ws = websocket.WebSocketApp(
            socket_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        logger.info("Bắt đầu vòng lặp Producer. Nhấn Ctrl+C để ngừng quá trình ép dữ liệu.")
        try:
            self.ws.run_forever()
        except KeyboardInterrupt:
            self.producer.close()
            logger.info("Người dùng yêu cầu thoát. Đã tắt an toàn Producer.")

def main() -> None:
    # Nạp các tuỳ chỉnh môi trường và bắt đầu chạy Kafka
    load_dotenv_file()
    producer = FinnhubCryptoProducer()
    producer.start()

if __name__ == "__main__":
    main()
