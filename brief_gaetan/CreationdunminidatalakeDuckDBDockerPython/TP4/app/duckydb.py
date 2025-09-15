import plotly.express as px
import streamlit as st
import pandas as pd
import duckdb as dd


st.set_page_config(layout="wide")

con = dd.connect("../outputs/weather.db")


col1, col2 = st.columns([2, 1])

# partie graph
with col1:

    map = con.sql(f'''
                SELECT * FROM weather;
                ''')

    templyon = con.sql(f'''
                SELECT temperature FROM weather WHERE city = 'Lyon';
                ''').df()

    temparis = con.sql(f'''
                SELECT temperature FROM weather WHERE city = 'Paris';
                ''').df()

    col11, col12 = st.columns ([1, 1])
    with col11:
        st.metric(label="Temperature a Lyon", value=templyon['temperature'][0])
    with col12:
        st.metric(label="Temperature a Paris", value=temparis['temperature'][0])

    fig = px.density_map(map, lat='latitude', lon='longitude', z='temperature', radius=10,
                            center=dict(lat=46.603354, lon=1.888334), zoom=5,
                            map_style="open-street-map")

    st.plotly_chart(fig)


# partie download + SQL
with col2:

    @st.cache_data
    def convert_for_download(df):
        return df.to_csv().encode("utf-8")

    input = st.text_input("input aisskuaill", "SELECT * FROM weather;")

    lol = con.sql(f'''{input}''').df()

    st.dataframe(lol)

    csv = convert_for_download(lol)

    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="data.csv",
        mime="text/csv",
        icon=":material/download:",
    )
