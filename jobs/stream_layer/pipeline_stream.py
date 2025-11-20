import time
import threading
import signal
import sys
import socket
from kafka_producer import send_message
from kafka_consumer import consum 
from stream_data import generate_real_time_data, choose_random_log_full

# Kết thúc chương trình khi nhận tín hiệu dừng
STOP = threading.Event()
def handle_sigint(signum, frame):
    STOP.set()
signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)

# Sinh dữ liệu mẫu từ file test_stream_data.csv mô phỏng real-time
result = generate_real_time_data()

# Kiểm tra Kafka đã sẵn sàng
def wait_for_kafka(host='127.0.0.1', port=29092, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline and not STOP.is_set():
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"✅ Kafka ready at {host}:{port}")
                return True
        except OSError:
            print("⏳ Waiting for Kafka...")
            time.sleep(2)
    return False

# Thread gửi message đến Kafka
def producer_thread():
    # gửi đúng 5 message rồi kết thúc
    for _ in range(5):
        if STOP.is_set():
            break
        try:
            # Lấy ngẫu nhiên 1 log_full từ result
            message = choose_random_log_full(result)
            # Gửi message đến Kafka
            send_message(message)         
            time.sleep(10)
        except Exception as e:
            print(f"Error in producer_thread: {e}")

# Thread giải quyết message từ Kafka
def consumer_thread():
    try:
        consum()         
    except Exception as e:
        print(f"Error in consumer_thread: {e}")

if __name__ == "__main__":
    # Đợi Kafka sẵn sàng trước khi khởi chạy threads
    if not wait_for_kafka():
        print("Kafka not ready. Exit.")
        sys.exit(1)

    t_producer = threading.Thread(target=producer_thread, name="producer")
    t_consumer = threading.Thread(target=consumer_thread, name="consumer", daemon=True)
    # consumer để daemon: app vẫn thoát được khi producer xong và Ctrl+C

    t_consumer.start()
    t_producer.start()

    t_producer.join()   # đợi gửi xong 5 message

    time.sleep(5)
    STOP.set()
    print("✅ Done.")
