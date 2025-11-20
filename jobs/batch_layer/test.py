import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:belieber@localhost:5432/db_analytics')
df = pd.read_sql('SELECT * FROM "log_full_labeled" LIMIT 20;', engine)
print(" Sample data from table Phone:")
print(df)