import pandas as pd
from sqlalchemy import create_engine

# Kết nối đến cơ sở dữ liệu PostgreSQL
# engine = create_engine('postgresql://postgres:belieber@localhost:5432/db_analytics')
engine = create_engine('postgresql://admin:admin@localhost:5432/bigdata_db')

# Đọc dữ liệu từ file CSV và lưu vào bảng log_full_labeled
df_log_full_labeled = pd.read_csv('Stock-Price\data\logs_full_labeled\logs_full_labeled.csv')
df_log_full_labeled.to_sql('log_full_labeled', engine, if_exists='replace', index=False)

print("Dữ liệu đã lưu vào bảng log_full_labeled")

# Đọc dữ liệu từ file CSV và lưu vào bảng log_prepared
df_log_prepared = pd.read_csv('Stock-Price\data\log_prepared\log_prepared.csv')
df_log_prepared.to_sql('log_prepared', engine, if_exists='replace', index=False)
print("Dữ liệu đã lưu vào bảng log_prepared")

