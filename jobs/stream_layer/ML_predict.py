import joblib
import json
import pandas as pd
import re
import numpy as np

# load model
model = joblib.load("../model_ML/lgb_hdfs_model.pkl")

with open("../model_ML/lgb_hdfs_meta.json") as f:
    meta = json.load(f)

feature_cols = meta["feature_cols"]   # Chỉ lấy danh sách cột đặc trưng đã train

LOG_TEMPLATES_PATH = "HDFS.log_templates.csv"  

df_templates = pd.read_csv(LOG_TEMPLATES_PATH)


assert "EventId" in df_templates.columns
assert "EventTemplate" in df_templates.columns

# Xử lý template thành regex
def template_to_regex(tmpl: str) -> str:
    escaped = re.escape(tmpl)
    regex = escaped.replace(r"\[\*\]", ".*")
    return regex

# Pre-compile regex cho nhanh
compiled_templates = []
for _, row in df_templates.iterrows():
    eid = row["EventId"]
    tmpl = row["EventTemplate"]
    pattern = template_to_regex(tmpl)
    compiled_templates.append((eid, re.compile(pattern)))

# Lấy danh sách tất cả EventId (E1..En) từ templates
all_event_ids = [eid for eid, _ in compiled_templates]

# Đếm số lần xuất hiện mỗi EventId trong log_full
def extract_event_counts_from_log(log_full: str):
    """
    Input: log_full (nhiều dòng)
    Output: dict { "E1": count, "E2": count, ... }
    """
    # Tách log_full thành từng dòng
    lines = log_full.split("\n")

    # Danh sách EventId match được
    matched_events = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Thử match với từng template
        matched = False
        for eid, pattern in compiled_templates:
            if pattern.search(line):
                matched_events.append(eid)
                matched = True
                break
        if not matched:
            # Không map được template nào 
            pass

    # Đếm số lần xuất hiện mỗi EventId
    event_counts = {eid: 0 for eid in all_event_ids}
    for eid in matched_events:
        if eid in event_counts:
            event_counts[eid] += 1

    return event_counts


# Dự đoán nhãn cho 1 block dựa trên dữ liệu đầu vào
def predict_block(data: dict, threshold: float ):
    """
    data: dict từ Kafka / input, ví dụ:
    {
        "BlockId": "...",
        "start_ts": "...",
        "end_ts": "...",
        "duration_sec": 0,
        "log_full": "dòng1\n dòng2\n...",
        "num_lines": 3
    }
    """

    log_full = data["log_full"]

    #Đếm số event trong log
    event_counts = extract_event_counts_from_log(log_full)

    #Build feature vector đúng với feature_cols đã train
    row_dict = {}

    for col in feature_cols:
        if col in event_counts:
            # Các cột E1..En
            row_dict[col] = event_counts[col]
        else:
            # Nếu trong feature_cols có thêm cột khác 
            row_dict[col] = data.get(col, 0)

    # Tạo DataFrame 1 dòng
    x = pd.DataFrame([row_dict], columns=feature_cols)
    x = x.astype("float32")

    #Dự đoán
    proba = model.predict_proba(x)[0, 1]
    pred = int(proba >= threshold)

    return pred

