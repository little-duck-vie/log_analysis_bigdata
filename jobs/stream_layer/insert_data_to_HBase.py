from datetime import datetime
import happybase
import pandas as pd
import time

def insert_dataHbase(data):

    connection = happybase.Connection('localhost', port=9090)
    connection.open()

    if b'log_stream_data' not in connection.tables():
        # Tạo bảng 'log_stream_data' 
        connection.create_table(
            'log_stream_data',
            {
                'info': dict()  # Column family 'info'
            }
        )


    table = connection.table('log_stream_data')

    # lấy thời gian hiện tại
    ts_ms = int(time.time() * 1000)
    current_time = datetime.now()

    # Tạo khóa chính (row key) dựa trên BlockId
    row_key = str(data['BlockId'])

    data_to_insert = {
        b'info:start_ts': bytes(str(data['start_ts']), 'utf-8'),
        b'info:end_ts': bytes(str(data['end_ts']), 'utf-8'),
        b'info:duration_sec': bytes(str(data['duration_sec']), 'utf-8'),
        b'info:log_full': bytes(str(data['log_full']), 'utf-8'),
        b'info:num_lines': bytes(str(data['num_lines']), 'utf-8'),
        b'info:features': bytes(str(data['features']), 'utf-8'),
        b'info:prediction': bytes(str(data['prediction']), 'utf-8'),
        b'info:timestamp': bytes(str(current_time), 'utf-8'),
        b'info:ts_ms': bytes(str(ts_ms), 'utf-8'), # thời gian (lưu) dùng để sắp xếp
    }

    # Ghi dữ liệu
    table.put(row_key.encode(), data_to_insert)
    print(f"[HBase] ✅ Inserted success")

    connection.close()
