import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
import sqlalchemy

HOST = "ca-pro-rds-prod.cllw6gj7ocf7.us-east-1.rds.amazonaws.com"
DB_NAME = "telegram_data"
DB_USER =  "analytics"
DB_PASS = "acaa301cf8214359b9bb9722b3aa469f"

engine = sqlalchemy.create_engine(f"postgres://{DB_USER}:{DB_PASS}@{HOST}:5432/{DB_NAME}")

chunk_size = 10**4

# Create Connection
# connection = pg.connect(f"host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASS}")

# List Databases
# df_chunks = pd.read_sql_query('SELECT datname FROM pg_database WHERE datistemplate = false', con=connection, chunksize=chunk_size)

# List Tables
# df_chunks = pd.read_sql_query('SELECT table_schema,table_name FROM information_schema.tables ORDER BY table_schema,table_name', con=connection, chunksize=chunk_size)

df_chunks = pd.read_sql_table("message_service_message_y2018d311", engine, chunksize=chunk_size)

for df in df_chunks:
    print(df.head())
    df.to_csv("crypto.csv", encoding="utf-8")