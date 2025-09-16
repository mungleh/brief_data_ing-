import plotly.express as px
import streamlit as st
import pandas as pd
import duckdb as dd
import os

con = dd.connect("../outputs/jo.db")

st.set_page_config(layout="wide")
st.title("📊 DuckDB Table Manager")

col1, col2 = st.columns([1, 2])

with col1:
    # --- Select table ---
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    selected_table = st.selectbox("Select a table to preview:", tables)

    if selected_table:
        safe_table_name = f'"{selected_table}"'

        # --- Export to Parquet ---
        parquet_path = os.path.join("../outputs", f"{selected_table}.parquet")
        if st.button(f"Download `{selected_table}` as Parquet"):
            full_df = con.execute(f"SELECT * FROM {safe_table_name}").fetchdf()
            full_df.to_parquet(parquet_path, engine="pyarrow", index=False)
            st.success(f"Exported `{selected_table}` → {parquet_path}")

        # --- Delete table ---
        if st.button(f"Delete table `{selected_table}`"):
            con.execute(f"DROP TABLE {safe_table_name}")
            st.success(f"Table `{selected_table}` deleted!")
            st.rerun()

    # --- CSV import ---
    st.header("📥 Import CSV")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    new_table_name = st.text_input("New table name (can start with numbers or special chars)")
    if uploaded_file and new_table_name and st.button("Import CSV"):
        safe_table_name = f'"{new_table_name}"'
        df_csv = pd.read_csv(uploaded_file)
        con.register("tmp_df", df_csv)
        con.execute(f"CREATE TABLE IF NOT EXISTS {safe_table_name} AS SELECT * FROM tmp_df")
        con.unregister("tmp_df")
        st.success(f"CSV imported as table `{new_table_name}`")
        st.rerun()

with col2:
    if selected_table:
        safe_table_name = f'"{selected_table}"'

        # --- Pick column for barplot ---
        df_sample = con.execute(f"SELECT * FROM {safe_table_name} LIMIT 100").fetchdf()
        categorical_cols = [c for c in df_sample.columns if df_sample[c].dtype == "object"]

        if categorical_cols:
            selected_column = st.selectbox("Select a column to plot:", categorical_cols)

            try:
                plot_df = con.execute(f"""
                    SELECT {selected_column} as col, COUNT(*) as count
                    FROM {safe_table_name}
                    GROUP BY {selected_column}
                    ORDER BY count DESC
                    LIMIT 20
                """).fetchdf()

                if not plot_df.empty:
                    st.subheader(f"📊 Distribution by `{selected_column}`")
                    fig = px.bar(
                        plot_df,
                        x="col",
                        y="count",
                        text="count",
                        title=f"Top categories in `{selected_column}`"
                    )
                    fig.update_traces(textposition="outside")
                    fig.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"⚠️ Column `{selected_column}` is empty.")
            except Exception as e:
                st.warning(f"Could not plot `{selected_column}`: {e}")
        else:
            st.info("⚠️ No categorical columns found to plot.")

if selected_table:
    safe_table_name = f'"{selected_table}"'
    df = con.execute(f"SELECT * FROM {safe_table_name} LIMIT 5").fetchdf()
    st.write(f"First 5 rows of `{selected_table}`:")
    st.dataframe(df)
