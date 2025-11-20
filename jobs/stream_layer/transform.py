import json
import re
from datetime import datetime
from typing import Dict, Any
import pandas as pd
import numpy as np

def transformation(data: str) -> Dict[str, Any]:
    payload = json.loads(data)

    # 1) Chuẩn bị log lines
    log_full = payload.get("log_full", "") or ""
    log_lines = log_full.split("\n")
    df = pd.DataFrame(log_lines, columns=["log_full"])

    # 2) Trích xuất trường bằng regex
    patterns = {
        "timestamp": r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
        "pid": r"\s(\d+)\s+(?=INFO|WARN|ERROR|DEBUG)",
        "level": r"(INFO|WARN|ERROR|DEBUG)",
        "component": r"(?:INFO|WARN|ERROR|DEBUG)\s+([A-Za-z0-9\$\.\*]+):",
        "message": r"(?:INFO|WARN|ERROR|DEBUG)\s+[A-Za-z0-9\$\.\*]+:\s*(.*)",
        "BlockId": r"(blk_[\-0-9]+)",
    }
    for col_name, pat in patterns.items():
        df[col_name] = df["log_full"].str.extract(pat, expand=False)

    # Parse timestamp để sort đúng
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Nạp template và ánh xạ message -> EventId
    df_templates = pd.read_csv("HDFS.log_templates.csv")  # cần có cột EventId, EventTemplate

    def template_to_regex(tmpl: str) -> re.Pattern:
        # Escape toàn bộ, rồi thay [*] thành .*? , và anchor ^...$
        escaped = re.escape(str(tmpl))
        regex = "^" + escaped.replace(r"\[\*\]", ".*?") + "$"
        return re.compile(regex)

    df["EventId"] = None
    templates = [(row["EventId"], template_to_regex(row["EventTemplate"])) 
                 for _, row in df_templates.iterrows()]

    for eid, pat in templates:
        # na=False để không bị NaN làm rơi match -> boolean Series
        mask = df["message"].str.match(pat, na=False)
        df.loc[mask, "EventId"] = eid

    #Gom theo BlockId, tạo EventSequence rồi đếm tần suất E1..E27
    df_sorted = df.sort_values(["BlockId", "timestamp"], na_position="last")

    # Loại bỏ dòng không có BlockId
    df_sorted = df_sorted[ df_sorted["BlockId"].notna() ]

    event_traces = (
        df_sorted.groupby("BlockId")["EventId"]
        .apply(lambda s: [x for x in s.tolist() if pd.notna(x)])
        .reset_index(name="EventSequence")
    )

    # Danh sách feature 
    features = [f"E{i}" for i in range(1, 30)]  

    # Tạo cột đếm cho từng E*
    for f in features:
        event_traces[f] = event_traces["EventSequence"].apply(lambda seq: seq.count(f))

    # DataFrame đặc trưng cuối cùng (chỉ BlockId + E1..E29)
    df_features = event_traces[["BlockId"] + features].copy()

    # Lấy block cần trả 
    block_id_req = payload.get("BlockId")
    even_list: list

    if block_id_req and not df_features[df_features["BlockId"] == block_id_req].empty:
        row = df_features[df_features["BlockId"] == block_id_req].iloc[0]
        even_list = [int(row[f]) for f in features]
        block_out = block_id_req
    elif not df_features.empty:
        row = df_features.iloc[0]
        even_list = [int(row[f]) for f in features]
        block_out = str(row["BlockId"])
    else:
        # Không tìm thấy block nào trong log
        even_list = [0] * len(features)
        block_out = block_id_req or "unknown"

    # 6) Trả kết quả
    return {
        "BlockId": block_out,
        "start_ts": payload.get("start_ts"),
        "end_ts": payload.get("end_ts"),
        "duration_sec": payload.get("duration_sec"),
        "log_full": log_full,
        "num_lines": len(log_lines),
        "features": even_list     
    }
