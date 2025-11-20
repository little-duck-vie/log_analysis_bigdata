"""
Get data from HBase - Simple version
Reads the latest record from HBase
"""
import happybase
import logging

logger = logging.getLogger(__name__)


def decode_dict(d):
    """Convert HBase bytes to readable dict"""
    result = {}
    for k, v in d.items():
        try:
            key = k.decode('utf-8')
            # Remove column family prefix (e.g., 'm:score' -> 'score')
            if ':' in key:
                key = key.split(':', 1)[1]
            value = v.decode('utf-8')
            result[key] = value
        except Exception as e:
            logger.error(f"Error decoding {k}: {e}")
            continue
    return result


def get_last_record_from_hbase():
    """
    Get the latest record from HBase table 'loghub:detections_v1'
    """
    try:
        # Connect to HBase
        connection = happybase.Connection('localhost', 9090)
        connection.open()
        
        # Open table
        table = connection.table('loghub:detections_v1')
        
        # Scan limit=1 in reverse (newest first)
        scanner = table.scan(limit=1, reverse=True)
        
        last_record = {}
        for key, data in scanner:
            last_record = decode_dict(data)
            # Add rowkey as detection_id
            last_record['detection_id'] = key.decode('utf-8')
            break
        
        scanner.close()
        connection.close()
        
        logger.info(f"Got record from HBase: {list(last_record.keys())}")
        return last_record
    
    except Exception as e:
        logger.error(f"Failed to get data from HBase: {e}")
        # Return empty dict or mock data on error
        return {
            'error': str(e),
            'message': 'Failed to connect to HBase'
        }


def get_latest_records_from_hbase(limit=50):
    """
    Get multiple latest records from HBase
    """
    try:
        connection = happybase.Connection('localhost', 9090)
        connection.open()
        
        table = connection.table('loghub:detections_v1')
        
        # Scan with limit and reverse
        scanner = table.scan(limit=limit, reverse=True)
        
        records = []
        for key, data in scanner:
            record = decode_dict(data)
            record['detection_id'] = key.decode('utf-8')
            records.append(record)
        
        scanner.close()
        connection.close()
        
        logger.info(f"Got {len(records)} records from HBase")
        return records
    
    except Exception as e:
        logger.error(f"Failed to get data from HBase: {e}")
        return []


def get_high_score_detections(threshold=0.2, limit=50):
    """
    Get high-score detections from HBase
    """
    try:
        connection = happybase.Connection('localhost', 9090)
        connection.open()
        
        table = connection.table('loghub:detections_v1')
        
        scanner = table.scan(limit=limit*2, reverse=True)  # Scan more to filter
        
        records = []
        for key, data in scanner:
            record = decode_dict(data)
            
            # Check score
            try:
                score = float(record.get('score', 0))
                if score >= threshold:
                    record['detection_id'] = key.decode('utf-8')
                    records.append(record)
            except (ValueError, TypeError):
                continue
            
            if len(records) >= limit:
                break
        
        scanner.close()
        connection.close()
        
        logger.info(f"Got {len(records)} high-score records")
        return records
    
    except Exception as e:
        logger.error(f"Failed to get high-score data from HBase: {e}")
        return []
