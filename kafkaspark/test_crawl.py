import websocket, json, time
from kafka import KafkaProducer

API_KEY = "d6g5sv1r01qt4931ljbgd6g5sv1r01qt4931ljc0"
SYMBOLS = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:BNBUSDT"]

# Cấu hình Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "trade":
        for trade in data["data"]:
            payload = {
                "symbol": trade.get("s"),
                "price": float(trade.get("p")),
                "volume": float(trade.get("v")),
                "timestamp": float(trade.get("t"))
            }
            producer.send('crawled_data', value=payload)
            print(f"🚀 [Kafka] Sent: {payload['symbol']} - {payload['price']}")

def on_open(ws):
    print("✅ Connected! Subscribing...")
    for s in SYMBOLS: ws.send(json.dumps({"type":"subscribe","symbol":s}))

if __name__ == "__main__":
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={'d7c703hr01qsv375hqugd7c703hr01qsv375hqv0'}", 
                                on_open=on_open, on_message=on_message)
    ws.run_forever()