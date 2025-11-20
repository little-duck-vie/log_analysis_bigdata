"""
HBase & Application Configuration
"""
import os

# HBase Configuration
HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT', 9090))
HBASE_TABLE = os.getenv('HBASE_TABLE', 'log_stream_data')  # Read from stream_layer table

# Flask Configuration
FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'True') == 'True'  # Enable debug by default
FLASK_PORT = int(os.getenv('FLASK_PORT', 5000))

# Socket.IO Configuration
SOCKETIO_CORS_ALLOWED_ORIGINS = os.getenv('SOCKETIO_CORS_ALLOWED_ORIGINS', '*')
SOCKETIO_MESSAGE_QUEUE = os.getenv('SOCKETIO_MESSAGE_QUEUE', None)  # Redis URL nếu cần distributed

# Detection Query Configuration
DEFAULT_LIMIT = int(os.getenv('DEFAULT_LIMIT', 100))  # Mặc định lấy 100 detections gần nhất
SCAN_BATCH_SIZE = int(os.getenv('SCAN_BATCH_SIZE', 1000))  # Batch size khi scan HBase
REALTIME_POLL_INTERVAL = int(os.getenv('REALTIME_POLL_INTERVAL', 1))  # Poll HBase every 1 second for real-time updates

# Watermark & Timestamp Configuration
WATERMARK_MINUTES = int(os.getenv('WATERMARK_MINUTES', 5))  # Cửa sổ chấp nhận late data (phút)

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
