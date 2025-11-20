"""
Flask + Socket.IO Backend - Loghub Detection Dashboard API
"""
import logging
import threading
import time
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
from config import (
    HBASE_HOST, HBASE_PORT, HBASE_TABLE, FLASK_PORT, FLASK_DEBUG,
    SOCKETIO_CORS_ALLOWED_ORIGINS, DEFAULT_LIMIT, REALTIME_POLL_INTERVAL,
    LOG_LEVEL
)
from hbase_client import HBaseClient

# Logging setup
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# Flask + Socket.IO initialization
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
app.config['TEMPLATES_AUTO_RELOAD'] = True  # Auto-reload templates
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # Disable caching for static files
app.jinja_env.auto_reload = True  # Force Jinja2 to reload templates
app.jinja_env.cache = {}  # Disable template cache
socketio = SocketIO(
    app,
    cors_allowed_origins=SOCKETIO_CORS_ALLOWED_ORIGINS,
    async_mode='threading',
    ping_timeout=60,
    ping_interval=25
)

# Global state
hbase_client = None
realtime_thread = None
realtime_active = False
connected_clients = {}


def init_hbase():
    """Khởi tạo HBase client"""
    global hbase_client
    try:
        hbase_client = HBaseClient(HBASE_HOST, HBASE_PORT, HBASE_TABLE)
        logger.info("HBase client initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize HBase client: {e}")
        return False


def realtime_poller():
    """
    Background thread để poll HBase định kỳ và emit updates qua Socket.IO
    """
    logger.info("Realtime poller started")
    last_check = {}
    
    while realtime_active:
        try:
            # Lấy latest detections (tăng limit lên 500 để hiển thị nhiều hơn)
            detections = hbase_client.get_latest_detections(limit=500)
            
            if detections:
                # Emit tới tất cả connected clients
                socketio.emit('detections_update', {
                    'timestamp': datetime.now().isoformat(),
                    'detections': detections,
                    'count': len(detections)
                }, to=None, skip_sid=True)  # Broadcast to all
                
                logger.debug(f"Emitted {len(detections)} detections to all clients")
            
            time.sleep(REALTIME_POLL_INTERVAL)
        
        except Exception as e:
            logger.error(f"Error in realtime poller: {e}")
            time.sleep(REALTIME_POLL_INTERVAL)


def start_realtime_poller():
    """Bắt đầu background thread để poll HBase"""
    global realtime_thread, realtime_active
    if not realtime_active:
        realtime_active = True
        realtime_thread = threading.Thread(target=realtime_poller, daemon=True)
        realtime_thread.start()
        logger.info("Realtime poller thread started")


def stop_realtime_poller():
    """Dừng background thread"""
    global realtime_active
    realtime_active = False
    logger.info("Realtime poller stopped")


# ============ SOCKET.IO EVENTS ============

@socketio.on('connect')
def handle_connect():
    """Client kết nối"""
    client_id = request.sid
    connected_clients[client_id] = {
        'connected_at': datetime.now().isoformat(),
        'user_agent': request.headers.get('User-Agent', 'Unknown')
    }
    
    logger.info(f"Client connected: {client_id} ({len(connected_clients)} clients total)")
    
    # Emit welcome message
    emit('connected', {
        'message': 'Connected to Loghub Detection Dashboard',
        'client_id': client_id,
        'server_time': datetime.now().isoformat()
    })
    
    # Poller đã chạy từ lúc khởi động, không cần start lại


@socketio.on('disconnect')
def handle_disconnect():
    """Client ngắt kết nối"""
    client_id = request.sid
    if client_id in connected_clients:
        del connected_clients[client_id]
    
    logger.info(f"Client disconnected: {client_id} ({len(connected_clients)} clients left)")
    
    # Không dừng poller - để chạy liên tục


@socketio.on('request_latest')
def handle_request_latest(data):
    """Client request N detections mới nhất"""
    try:
        limit = data.get('limit', DEFAULT_LIMIT)
        tenant = data.get('tenant', None)
        
        detections = hbase_client.get_latest_detections(limit=limit, tenant=tenant)
        
        emit('latest_detections', {
            'detections': detections,
            'count': len(detections),
            'timestamp': datetime.now().isoformat(),
            'request': data
        })
        
        logger.info(f"Sent {len(detections)} latest detections to {request.sid}")
    
    except Exception as e:
        logger.error(f"Error in request_latest: {e}")
        emit('error', {'message': str(e)})


@socketio.on('request_by_filter')
def handle_request_by_filter(data):
    """Client request detections với filter: tenant, host, path"""
    try:
        tenant = data.get('tenant')
        host = data.get('host')
        path = data.get('path')
        limit = data.get('limit', DEFAULT_LIMIT)
        
        if not all([tenant, host, path]):
            emit('error', {'message': 'Missing required fields: tenant, host, path'})
            return
        
        detections = hbase_client.get_detections_by_tenant_host_path(
            tenant, host, path, limit=limit
        )
        
        emit('filtered_detections', {
            'detections': detections,
            'count': len(detections),
            'filter': {'tenant': tenant, 'host': host, 'path': path},
            'timestamp': datetime.now().isoformat()
        })
        
        logger.info(f"Sent {len(detections)} filtered detections to {request.sid}")
    
    except Exception as e:
        logger.error(f"Error in request_by_filter: {e}")
        emit('error', {'message': str(e)})


@socketio.on('request_by_time_range')
def handle_request_by_time_range(data):
    """Client request detections trong khoảng thời gian"""
    try:
        start_ts_ms = data.get('start_ts_ms')
        end_ts_ms = data.get('end_ts_ms')
        limit = data.get('limit', 1000)
        
        if not all([start_ts_ms, end_ts_ms]):
            emit('error', {'message': 'Missing required fields: start_ts_ms, end_ts_ms'})
            return
        
        # Lấy detections trong time range
        detections = hbase_client.get_detections_by_time_range_simple(
            start_ts_ms, end_ts_ms, limit=limit
        )
        
        emit('time_range_detections', {
            'detections': detections,
            'count': len(detections),
            'time_range': {'start_ms': start_ts_ms, 'end_ms': end_ts_ms},
            'timestamp': datetime.now().isoformat()
        })
        
        logger.info(f"Sent {len(detections)} time-range detections to {request.sid}")
    
    except Exception as e:
        logger.error(f"Error in request_by_time_range: {e}")
        emit('error', {'message': str(e)})


# ============ REST API ENDPOINTS ============

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'connected_clients': len(connected_clients)
    }), 200


@app.route('/api/latest', methods=['GET'])
def api_get_latest():
    """REST API: Lấy N detections mới nhất"""
    try:
        limit = request.args.get('limit', DEFAULT_LIMIT, type=int)
        tenant = request.args.get('tenant', None, type=str)
        
        detections = hbase_client.get_latest_detections(limit=limit, tenant=tenant)
        
        return jsonify({
            'success': True,
            'data': detections,
            'count': len(detections),
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Error in api_get_latest: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/by-filter', methods=['GET'])
def api_get_by_filter():
    """REST API: Lấy detections với filter tenant/host/path"""
    try:
        tenant = request.args.get('tenant', type=str)
        host = request.args.get('host', type=str)
        path = request.args.get('path', type=str)
        limit = request.args.get('limit', DEFAULT_LIMIT, type=int)
        
        if not all([tenant, host, path]):
            return jsonify({
                'success': False,
                'error': 'Missing required params: tenant, host, path'
            }), 400
        
        detections = hbase_client.get_detections_by_tenant_host_path(
            tenant, host, path, limit=limit
        )
        
        return jsonify({
            'success': True,
            'data': detections,
            'count': len(detections),
            'filter': {'tenant': tenant, 'host': host, 'path': path},
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Error in api_get_by_filter: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/by-time-range', methods=['GET'])
def api_get_by_time_range():
    """REST API: Lấy detections theo time range"""
    try:
        tenant = request.args.get('tenant', type=str)
        host = request.args.get('host', type=str)
        path = request.args.get('path', type=str)
        start_ts_ms = request.args.get('start_ts_ms', type=int)
        end_ts_ms = request.args.get('end_ts_ms', type=int)
        limit = request.args.get('limit', 1000, type=int)
        
        if not all([tenant, host, path, start_ts_ms, end_ts_ms]):
            return jsonify({
                'success': False,
                'error': 'Missing required params'
            }), 400
        
        detections = hbase_client.get_detections_by_time_range(
            tenant, host, path, start_ts_ms, end_ts_ms, limit=limit
        )
        
        return jsonify({
            'success': True,
            'data': detections,
            'count': len(detections),
            'time_range': {'start_ms': start_ts_ms, 'end_ms': end_ts_ms},
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Error in api_get_by_time_range: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/high-score', methods=['GET'])
def api_get_high_score():
    """REST API: Lấy detections có score cao (anomaly detection)"""
    try:
        threshold = request.args.get('threshold', 0.2, type=float)
        limit = request.args.get('limit', DEFAULT_LIMIT, type=int)
        
        detections = hbase_client.get_high_score_detections(threshold, limit)
        
        return jsonify({
            'success': True,
            'data': detections,
            'count': len(detections),
            'threshold': threshold,
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Error in api_get_high_score: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/stats', methods=['GET'])
def api_get_stats():
    """REST API: Lấy statistics"""
    try:
        stats = hbase_client.get_detection_stats()
        
        return jsonify({
            'success': True,
            'data': stats,
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Error in api_get_stats: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/delete/<detection_id>', methods=['DELETE'])
def api_delete_detection(detection_id):
    """REST API: Xóa một detection"""
    try:
        success = hbase_client.delete_detection(detection_id)
        
        return jsonify({
            'success': success,
            'detection_id': detection_id,
            'timestamp': datetime.now().isoformat()
        }), 200 if success else 500
    
    except Exception as e:
        logger.error(f"Error in api_delete_detection: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/delete-all', methods=['DELETE'])
def api_delete_all():
    """REST API: Xóa TẤT CẢ detections (NGUY HIỂM!)"""
    try:
        count = hbase_client.delete_all_detections()
        
        return jsonify({
            'success': True,
            'deleted_count': count,
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Error in api_delete_all: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/', methods=['GET'])
def index():
    """Serve dashboard HTML (bilingual support)"""
    return render_template('dashboard.html')


@app.route('/en', methods=['GET'])
def english_dashboard():
    """Redirect to main dashboard (now supports language switching)"""
    return render_template('dashboard.html')


@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Not found'
    }), 404


@app.errorhandler(500)
def server_error(error):
    logger.error(f"Server error: {error}")
    return jsonify({
        'success': False,
        'error': 'Internal server error'
    }), 500


# ============ APPLICATION LIFECYCLE ============

def main():
    """Run Flask + Socket.IO server"""
    logger.info(f"Starting Loghub Detection Dashboard...")
    logger.info(f"HBase: {HBASE_HOST}:{HBASE_PORT}/{HBASE_TABLE}")
    
    # Initialize HBase
    if not init_hbase():
        logger.error("Failed to initialize HBase. Exiting.")
        return
    
    # Bắt đầu realtime poller ngay từ đầu (không đợi client)
    start_realtime_poller()
    logger.info("Realtime polling started (always-on mode)")
    
    try:
        # Run Flask app
        socketio.run(
            app,
            host='0.0.0.0',
            port=FLASK_PORT,
            debug=FLASK_DEBUG,
            use_reloader=False,  # Tắt auto-reloader để tránh double-startup
            allow_unsafe_werkzeug=FLASK_DEBUG  # Chỉ cho phép ở dev mode
        )
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    
    finally:
        stop_realtime_poller()
        if hbase_client:
            hbase_client.close()
        logger.info("Application stopped")


if __name__ == '__main__':
    main()
