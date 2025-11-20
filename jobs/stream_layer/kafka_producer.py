import json
from kafka import KafkaProducer
import socket, time

# Cấu hình Kafka Producer
BOOTSTRAP = ['127.0.0.1:29092']  
TOPIC = 'log_stream_topic'

# Tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
    acks='all',
    linger_ms=5,
    retries=5,
    request_timeout_ms=30000,
    api_version=(3, 5, 1),   
    security_protocol='PLAINTEXT',
)

# Hàm gửi message đến Kafka
def send_message(message: dict):
    try:
        producer.send(TOPIC, message)
        producer.flush()
        print("[Kafka Producer] ✅ đẩy message thành công")
    except Exception as error:
        print(f"[Kafka Producer] ❌ Error: {error}")
