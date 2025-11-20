import pandas as pd
import random
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Tạo đường dẫn tương đối
PATH_LOG_FULL = os.path.normpath(
    os.path.join(BASE_DIR, "..", "batch_layer", "batch_analysis", "data", "logs_full_labeled", "logs_full_labeled.csv")
)
PATH_TEST_CSV = os.path.normpath(
    os.path.join(BASE_DIR, "..", "model_ML", "test_stream_data.csv")
)

# Tạo dữ liệu mẫu từ file test_stream_data.csv mô phỏng real-time
def generate_real_time_data(
    path_log_full=PATH_LOG_FULL,
    path_test_csv=PATH_TEST_CSV,
):
    # Đọc file CSV full log
    df_full = pd.read_csv(path_log_full)

    # Đọc file test để lấy list BlockId
    df_test = pd.read_csv(path_test_csv)
    block_ids = df_test["BlockId"].dropna().unique().tolist()

    # Lọc các dòng có BlockId thuộc test set
    df = df_full[df_full["BlockId"].isin(block_ids)].copy()

    # Nếu không có bản ghi nào khớp → trả về DataFrame rỗng
    if df.empty:
        print("[WARN] Không tìm thấy BlockId nào trong logs_full_labeled khớp với test_stream_data")
        return df  # df rỗng

    # Chỉ giữ các cột cần thiết
    result = df[["BlockId", "start_ts", "end_ts", "duration_sec", "log_full", "num_lines"]]
    return result

# Chọn ngẫu nhiên 1 log_full từ result 
def choose_random_log_full(result: pd.DataFrame):
    if result is None or result.empty:
        raise ValueError("DataFrame rỗng, không thể chọn ngẫu nhiên log_full")

    n = random.randint(0, len(result) - 1)
    return result.iloc[n].to_dict()



