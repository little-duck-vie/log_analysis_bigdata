from kafka import KafkaConsumer
import json
from transform import transformation
from insert_data_to_HBase import insert_dataHbase
from ML_predict import predict_block

# Cấu hình Kafka Consumer 
BOOTSTRAP = ['127.0.0.1:29092']
TOPIC = 'log_stream_topic'

# Xử lý message từ Kafka
def consum():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id='my_consumer_group',
        enable_auto_commit=True,
        auto_offset_reset='latest',  
        value_deserializer=lambda b: b.decode('utf-8'),
        request_timeout_ms=30000,
        api_version=(3, 5, 1),
        security_protocol='PLAINTEXT',
        max_poll_interval_ms=300000,
        session_timeout_ms=15000,
    )

    for msg in consumer:
        try:
            data = msg.value             # lúc này đã là dict JSON
            print('[Kafka Comsumer] ✅ Get data from kafka success')
            # Hàm xử lý dữ liệu
            res = transformation(data)   # truyền dict cho transform
            data = json.loads(data)
            # Dự đoán nhãn với ngưỡng tốt nhất cho tập test 0.83
            prediction = predict_block(data, 0.83)             
            res['prediction'] = prediction   
            insert_dataHbase(res)
            print("-------------------")
        except Exception as e:
            print(f"Error processing message: {e}")
