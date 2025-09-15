import plotly.express as px
import streamlit as st
import pandas as pd
import duckdb as dd
import os

# --- DuckDB connection ---
con = dd.connect("../outputs/jo.db")

# --- Import Parquet files into DuckDB ---
files = con.execute("""
    SELECT DISTINCT filename
    FROM read_parquet('data/*.parquet', filename=True)
""").fetchall()

for f in files:
    file_path = f[0]
    table_name = os.path.basename(file_path).replace(".parquet", "")
    safe_table_name = f'"{table_name}"'  # <-- quotes added

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {safe_table_name} AS
        SELECT *
        FROM read_parquet('{file_path}')
    """)
    print(f"Imported {file_path} → {table_name}")

# --- Streamlit config ---
st.set_page_config(layout="wide")
st.title("📊 DuckDB Table Manager")

# --- List tables ---
tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]

if tables:
    selected_table = st.selectbox("Select a table to preview:", tables)

    if selected_table:
        safe_table_name = f'"{selected_table}"'  # <-- quotes added
        df = con.execute(f"SELECT * FROM {safe_table_name} LIMIT 5").fetchdf()
        st.write(f"First 5 rows of `{selected_table}`:")
        st.dataframe(df)

        # --- Export to Parquet ---
        parquet_path = os.path.join("../outputs", f"{selected_table}.parquet")
        if st.button(f"Download `{selected_table}` as Parquet"):
            full_df = con.execute(f"SELECT * FROM {safe_table_name}").fetchdf()
            full_df.to_parquet(parquet_path, engine="pyarrow", index=False)
            st.success(f"Exported `{selected_table}` → {parquet_path}")

        # --- Delete table button ---
        if st.button(f"Delete table `{selected_table}`"):
            con.execute(f"DROP TABLE {safe_table_name}")  # <-- quotes added
            st.success(f"Table `{selected_table}` deleted!")
            st.experimental_rerun()

st.header("📥 Import CSV as Table")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
new_table_name = st.text_input("New table name (can start with numbers or special chars)")

if uploaded_file and new_table_name:
    if st.button("Import CSV"):
        safe_table_name = f'"{new_table_name}"'  # <-- quotes added

        df_csv = pd.read_csv(uploaded_file)
        con.register("tmp_df", df_csv)
        con.execute(f"CREATE TABLE IF NOT EXISTS {safe_table_name} AS SELECT * FROM tmp_df")
        con.unregister("tmp_df")

        st.success(f"CSV imported as table `{new_table_name}`")
        st.experimental_rerun()
