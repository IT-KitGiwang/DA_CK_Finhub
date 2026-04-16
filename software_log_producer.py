from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

log_levels = ["INFO", "WARNING", "ERROR", "DEBUG"]
components = ["AuthService", "Database", "PaymentGateway", "FrontendAPI"]
messages = ["User logged in", "Connection timeout", "Query executed", "Failed to load resource"]

print("Ready Software Log into Kafka. Press Ctrl+C to stop...")

try:
    while True:
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "level": random.choice(log_levels),
            "component": random.choice(components),
            "message": random.choice(messages)
        }
        
        producer.send('dack_streaming', log_entry)
        print(f"[Đã gửi] {log_entry['level']} - {log_entry['component']}")
        
        time.sleep(2)
        
except KeyboardInterrupt:
    print("\nThe bump has been turn off.")
finally:
    producer.flush()
    producer.close()