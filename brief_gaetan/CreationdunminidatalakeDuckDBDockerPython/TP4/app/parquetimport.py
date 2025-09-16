import duckdb as dd
import os

con = dd.connect("../outputs/jo.db")

# --- Import Parquet files ---
files = con.execute("""
    SELECT DISTINCT filename
    FROM read_parquet('data/*.parquet', filename=True)
""").fetchall()

for f in files:
    file_path = f[0]
    table_name = os.path.basename(file_path).replace(".parquet", "")
    safe_table_name = f'"{table_name}"'
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {safe_table_name} AS
        SELECT *
        FROM read_parquet('{file_path}')
    """)
    print(f"Imported {file_path} → {table_name}")
