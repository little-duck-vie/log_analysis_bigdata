"""
HBase Client - Query detections từ HBase
"""
import logging
import happybase
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import struct
import threading
import time

logger = logging.getLogger(__name__)


class HBaseClient:
    """
    Client để query detections từ HBase table: loghub:detections_v1
    
    RowKey format: tenant#host#path#(Long.MAX_VALUE - ts_ms)
    - Để scan ra mới nhất trước (descending by timestamp)
    
    Column Families:
    - m: metrics (err_1m, cnt_1m, p95_1m, score)
    - r: rules (rule_error_burst)
    - p: predictions (ML features - tuỳ chọn)
    """
    
    def __init__(self, host: str = 'localhost', port: int = 9090, table_name: str = 'loghub:detections_v1'):
        """
        Khởi tạo HBase connection
        
        Args:
            host: HBase Thrift server hostname
            port: HBase Thrift server port
            table_name: Tên HBase table (namespace:table)
        """
        self.host = host
        self.port = port
        self.table_name = table_name
        self.connection = None
        self.table = None
        self._connect()
    
    def _connect(self):
        """Kết nối tới HBase - Đơn giản"""
        try:
            self.connection = happybase.Connection(
                self.host, 
                self.port, 
                timeout=30000,
                autoconnect=True
            )
            self.table = self.connection.table(self.table_name)
            logger.info(f"Connected to HBase: {self.host}:{self.port}/{self.table_name}")
        except Exception as e:
            logger.error(f"Failed to connect to HBase: {e}")
            raise
    
    def _ensure_connection(self):
        """Đảm bảo connection còn sống"""
        if not self.connection or not self.table:
            try:
                self._connect()
            except:
                pass
    
    def close(self):
        """Đóng connection"""
        if self.connection:
            self.connection.close()
            logger.info("HBase connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    @staticmethod
    def _decode_long(data: bytes) -> int:
        """Decode signed long (8 bytes) từ bytes"""
        return struct.unpack('>q', data)[0]
    
    @staticmethod
    def _encode_long(value: int) -> bytes:
        """Encode signed long (8 bytes) thành bytes"""
        return struct.pack('>q', value)
    
    @staticmethod
    def _ts_ms_to_inverted(ts_ms: int) -> int:
        """Convert timestamp (ms) to inverted long: Long.MAX_VALUE - ts_ms"""
        MAX_LONG = 9223372036854775807  # Long.MAX_VALUE
        return MAX_LONG - ts_ms
    
    @staticmethod
    def _inverted_to_ts_ms(inverted: int) -> int:
        """Convert inverted long back to timestamp (ms)"""
        MAX_LONG = 9223372036854775807
        return MAX_LONG - inverted
    
    def _parse_rowkey(self, rowkey: bytes) -> Optional[Dict]:
        """
        Parse rowkey: tenant#host#path#inverted_ts
        
        Returns:
            Dict với keys: tenant, host, path, ts_ms
        """
        try:
            parts = rowkey.decode('utf-8').split('#')
            if len(parts) < 4:
                return None
            
            tenant, host, path, inverted_str = parts[0], parts[1], parts[2], parts[3]
            inverted = int(inverted_str)
            ts_ms = self._inverted_to_ts_ms(inverted)
            
            return {
                'tenant': tenant,
                'host': host,
                'path': path,
                'ts_ms': ts_ms,
                'ts': datetime.fromtimestamp(ts_ms / 1000.0).isoformat()
            }
        except Exception as e:
            logger.warning(f"Failed to parse rowkey {rowkey}: {e}")
            return None
    
    def _parse_detection(self, rowkey: bytes, data: Dict[bytes, bytes]) -> Optional[Dict]:
        """
        Parse một detection record từ HBase
        
        Args:
            rowkey: Row key từ HBase
            data: Column families data từ HBase
        
        Returns:
            Dict với keys: tenant, host, path, ts_ms, ts, metrics, rules
        """
        parsed_key = self._parse_rowkey(rowkey)
        # If rowkey doesn't follow tenant#host#path#inverted_ts format (e.g. BlockId rows),
        # still parse columns and return a best-effort detection object.
        if parsed_key:
            detection = parsed_key.copy()
        else:
            detection = {
                'tenant': None,
                'host': None,
                'path': None,
                'ts_ms': None,
                'ts': None
            }
        detection['metrics'] = {}
        detection['rules'] = {}
        detection['raw'] = {}
        detection['detection_id'] = rowkey.decode('utf-8')

        # The 'data' from happybase is a flat dict: b'family:qual' -> bytes
        # Iterate all columns and populate structured fields and a raw map
        try:
            for col_bytes, val_bytes in data.items():
                try:
                    col = col_bytes.decode('utf-8')  # e.g. 'm:score' or 'r:rule_error_burst'
                except Exception:
                    col = str(col_bytes)

                # qualifier parsing
                if ':' in col:
                    family, qual = col.split(':', 1)
                else:
                    family, qual = ('', col)

                # decode value
                try:
                    val_decoded = val_bytes.decode('utf-8')
                except Exception:
                    # fallback to repr
                    val_decoded = repr(val_bytes)

                # store raw
                detection['raw'][f"{family}:{qual}"] = val_decoded

                # try to place into metrics/rules when appropriate
                if family == 'm':
                    # common numeric metrics
                    if qual in ['err_1m', 'cnt_1m', 'p95_1m', 'score']:
                        try:
                            detection['metrics'][qual] = float(val_decoded)
                        except Exception:
                            detection['metrics'][qual] = val_decoded
                    else:
                        # try numeric, otherwise keep string
                        try:
                            detection['metrics'][qual] = float(val_decoded)
                        except Exception:
                            detection['metrics'][qual] = val_decoded

                elif family == 'r':
                    # rules often integers/flags
                    try:
                        detection['rules'][qual] = int(val_decoded)
                    except Exception:
                        detection['rules'][qual] = val_decoded
                elif family == 'info':
                    # special handling for legacy stream_layer 'info' family
                    # common qualifiers: start_ts, end_ts, duration_sec, log_full, num_lines, features, prediction
                    key = qual
                    # Initialize fields dict if not exists
                    if 'fields' not in detection:
                        detection['fields'] = {}
                    
                    # try to parse numeric fields
                    if key in ['duration_sec', 'num_lines', 'prediction']:
                        try:
                            detection['fields'][key] = int(val_decoded)
                        except Exception:
                            try:
                                detection['fields'][key] = float(val_decoded)
                            except Exception:
                                detection['fields'][key] = val_decoded
                    elif key == 'ts_ms':
                        # 🆕 Extract HBase update time (ts_ms) - this is the insertion time
                        try:
                            detection['ts_ms'] = int(val_decoded)
                            logger.debug(f"Extracted ts_ms: {detection['ts_ms']}")
                        except Exception:
                            detection['ts_ms'] = None
                        detection['fields'][key] = val_decoded
                    elif key in ['start_ts', 'end_ts']:
                        # try parse datetime and store both in fields and top-level
                        try:
                            dt = datetime.fromisoformat(val_decoded)
                        except Exception:
                            try:
                                dt = datetime.strptime(val_decoded, '%Y-%m-%d %H:%M:%S')
                            except Exception:
                                dt = None
                        if dt:
                            detection[key] = dt.isoformat()
                            ms = int(dt.timestamp() * 1000)
                            # Only set ts_ms from start_ts/end_ts if not already set from ts_ms column
                            if not detection.get('ts_ms'):
                                detection['ts_ms'] = ms
                            detection['ts'] = detection.get('ts') or dt.isoformat()
                        # Store in fields as well for easy access
                        detection['fields'][key] = val_decoded
                    else:
                        # Store all other info:* fields
                        detection['fields'][key] = val_decoded

                else:
                    # For other families, try best-effort numeric decode
                    try:
                        num = float(val_decoded)
                        detection.setdefault('fields', {})[f"{family}:{qual}"] = num
                    except Exception:
                        detection.setdefault('fields', {})[f"{family}:{qual}"] = val_decoded

        except Exception as e:
            logger.debug(f"Failed parsing columns for {rowkey}: {e}")

        return detection
    
    def get_latest_detections(self, limit: int = 100, tenant: Optional[str] = None) -> List[Dict]:
        """
        Lấy N detections mới nhất - Đơn giản
        
        Args:
            limit: Số detections cần lấy (default 100)
            tenant: Filter theo tenant (optional)
        
        Returns:
            List of detections (sorted by timestamp, newest first)
        """
        try:
            # Create fresh connection for each request to avoid corruption
            connection = happybase.Connection(
                self.host,
                self.port,
                timeout=10000,
                autoconnect=True
            )
            table = connection.table(self.table_name)
            
            prefix = f"{tenant}#" if tenant else None
            scan_kwargs = {
                'limit': limit * 2,
                'batch_size': 100
            }
            if prefix:
                scan_kwargs['row_prefix'] = prefix.encode('utf-8')
            
            detections = []
            for rowkey, data in table.scan(**scan_kwargs):
                if len(detections) >= limit:
                    break
                detection = self._parse_detection(rowkey, data)
                if detection:
                    detections.append(detection)
            
            connection.close()
            
            detections.sort(key=lambda x: x.get('ts_ms', 0) or 0, reverse=True)
            return detections
        
        except Exception as e:
            logger.error(f"Error getting detections: {e}")
            return []
    
    def get_detections_by_tenant_host_path(
        self, 
        tenant: str, 
        host: str, 
        path: str, 
        limit: int = 100
    ) -> List[Dict]:
        """
        Lấy detections cho một specific tenant/host/path
        
        Args:
            tenant: Tenant name
            host: Host name
            path: Log path
            limit: Số records cần lấy
        
        Returns:
            List of detections (sorted by timestamp, newest first)
        """
        try:
            self._ensure_connection()
            
            prefix = f"{tenant}#{host}#{path}#".encode('utf-8')
            detections = []
            
            for rowkey, data in self.table.scan(row_prefix=prefix, limit=limit * 2, batch_size=100):
                if len(detections) >= limit:
                    break
                detection = self._parse_detection(rowkey, data)
                if detection:
                    detections.append(detection)
            
            detections.sort(key=lambda x: x.get('ts_ms', 0) or 0, reverse=True)
            return detections
        
        except Exception as e:
            logger.error(f"Error getting detections: {e}")
            return []
    
    def get_detections_by_time_range(
        self,
        tenant: str,
        host: str,
        path: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 1000
    ) -> List[Dict]:
        """
        Lấy detections trong một khoảng thời gian
        
        Args:
            tenant: Tenant name
            host: Host name
            path: Log path
            start_ts_ms: Thời gian bắt đầu (milliseconds)
            end_ts_ms: Thời gian kết thúc (milliseconds)
            limit: Max records
        
        Returns:
            List of detections (sorted by timestamp, newest first)
        """
        detections = []
        count = 0
        
        try:
            # Inverted timestamps
            inverted_start = self._ts_ms_to_inverted(start_ts_ms)
            inverted_end = self._ts_ms_to_inverted(end_ts_ms)
            
            # Scan từ end_ts trở lại start_ts (vì RowKey lưu inverted, nên end < start)
            prefix = f"{tenant}#{host}#{path}#".encode('utf-8')
            
            for rowkey, data in self.table.scan(row_prefix=prefix, limit=limit * 2, batch_size=1000):
                if count >= limit:
                    break
                
                detection = self._parse_detection(rowkey, data)
                if detection and start_ts_ms <= detection['ts_ms'] <= end_ts_ms:
                    detections.append(detection)
                    count += 1
            
            logger.info(f"Retrieved {len(detections)} detections in time range")
            return detections
        
        except Exception as e:
            logger.error(f"Failed to get detections by time range: {e}")
            return []
    
    def get_high_score_detections(self, score_threshold: float = 0.2, limit: int = 100) -> List[Dict]:
        """
        Lấy detections có score cao (phát hiện anomaly)
        
        Args:
            score_threshold: Ngưỡng score (default 0.2)
            limit: Số records cần lấy
        
        Returns:
            List of detections với score >= threshold
        """
        high_score = []
        count = 0
        
        try:
            self._ensure_connection()  # Đảm bảo connection còn sống
            
            for rowkey, data in self.table.scan(limit=limit * 10, batch_size=1000):
                if count >= limit:
                    break
                
                detection = self._parse_detection(rowkey, data)
                if detection:
                    score = detection.get('metrics', {}).get('score', 0)
                    if score >= score_threshold:
                        high_score.append(detection)
                        count += 1
            
            logger.info(f"Retrieved {len(high_score)} high-score detections (threshold={score_threshold})")
            return high_score
        
        except Exception as e:
            logger.error(f"Failed to get high-score detections: {e}")
            return []
    
    def get_detections_by_time_range_simple(
        self,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 1000
    ) -> List[Dict]:
        """
        Lấy detections trong khoảng thời gian (simple version - không cần tenant/host/path)
        Filter by start_ts field trong HBase
        
        Args:
            start_ts_ms: Start timestamp (milliseconds)
            end_ts_ms: End timestamp (milliseconds)
            limit: Số records tối đa
        
        Returns:
            List of detections trong time range
        """
        detections = []
        count = 0
        
        try:
            # Ensure connection
            self._ensure_connection()
            
            # Scan toàn bộ table và filter by timestamp
            for rowkey, data in self.table.scan(limit=limit * 5, batch_size=1000):
                if count >= limit:
                    break
                
                detection = self._parse_detection(rowkey, data)
                if detection:
                    # Check if detection has timestamp in range
                    det_ts_ms = detection.get('ts_ms')
                    if det_ts_ms and start_ts_ms <= det_ts_ms <= end_ts_ms:
                        detections.append(detection)
                        count += 1
            
            logger.info(f"Retrieved {len(detections)} detections in time range [{start_ts_ms}, {end_ts_ms}]")
            return detections
        
        except Exception as e:
            logger.error(f"Failed to get detections by time range: {e}")
            # Retry once
            try:
                logger.info("Retrying time range query...")
                time.sleep(0.5)  # Wait 500ms before retry
                self._connect()
                detections = []
                count = 0
                for rowkey, data in self.table.scan(limit=limit * 5, batch_size=1000):
                    if count >= limit:
                        break
                    detection = self._parse_detection(rowkey, data)
                    if detection:
                        det_ts_ms = detection.get('ts_ms')
                        if det_ts_ms and start_ts_ms <= det_ts_ms <= end_ts_ms:
                            detections.append(detection)
                            count += 1
                logger.info(f"Retry successful: {len(detections)} detections")
                return detections
            except:
                return []
    
    def delete_detection(self, detection_id: str) -> bool:
        """
        Xóa một detection khỏi HBase
        
        Args:
            detection_id: Row key của detection cần xóa
        
        Returns:
            True nếu xóa thành công, False nếu thất bại
        """
        try:
            connection = happybase.Connection(
                self.host,
                self.port,
                timeout=10000,
                autoconnect=True
            )
            table = connection.table(self.table_name)
            table.delete(detection_id.encode('utf-8'))
            connection.close()
            logger.info(f"Deleted detection: {detection_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete detection {detection_id}: {e}")
            return False
    
    def delete_all_detections(self) -> int:
        """
        Xóa TẤT CẢ detections trong HBase (CẨN THẬN!)
        
        Returns:
            Số lượng records đã xóa
        """
        try:
            connection = happybase.Connection(
                self.host,
                self.port,
                timeout=10000,
                autoconnect=True
            )
            table = connection.table(self.table_name)
            
            count = 0
            batch = table.batch()
            for rowkey, _ in table.scan():
                batch.delete(rowkey)
                count += 1
                if count % 100 == 0:
                    batch.send()
                    batch = table.batch()
            
            batch.send()
            connection.close()
            logger.warning(f"Deleted ALL {count} detections from HBase")
            return count
        except Exception as e:
            logger.error(f"Failed to delete all detections: {e}")
            return 0
    
    def get_detection_stats(self) -> Dict:
        """
        Lấy statistics về detections (tổng số records, unique tenants, etc.)
        
        Returns:
            Dict với stats
        """
        stats = {
            'total_detections': 0,
            'unique_tenants': set(),
            'unique_hosts': set(),
            'unique_paths': set(),
            'high_score_count': 0,
            'avg_score': 0.0
        }
        
        try:
            self._ensure_connection()  # Đảm bảo connection còn sống
            
            total_score = 0
            
            for rowkey, data in self.table.scan(limit=10000, batch_size=1000):
                detection = self._parse_detection(rowkey, data)
                if detection:
                    stats['total_detections'] += 1
                    t = detection.get('tenant')
                    h = detection.get('host')
                    p = detection.get('path')
                    if t:
                        stats['unique_tenants'].add(t)
                    if h:
                        stats['unique_hosts'].add(h)
                    if p:
                        stats['unique_paths'].add(p)

                    score = detection.get('metrics', {}).get('score', 0)
                    try:
                        total_score += float(score)
                    except Exception:
                        pass

                    if isinstance(score, (int, float)) and score >= 0.2:
                        stats['high_score_count'] += 1
            
            if stats['total_detections'] > 0:
                stats['avg_score'] = total_score / stats['total_detections']
            
            # Convert sets to lists for JSON serialization
            stats['unique_tenants'] = list(stats['unique_tenants'])
            stats['unique_hosts'] = list(stats['unique_hosts'])
            stats['unique_paths'] = list(stats['unique_paths'])
            
            logger.info(f"Stats: {stats['total_detections']} total detections")
            return stats
        
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return stats
