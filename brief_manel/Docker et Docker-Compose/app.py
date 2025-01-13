import plotly.express as px
import streamlit as st
import pandas as pd

conn = st.connection("postgresql", type="sql")

client = conn.query("SELECT * FROM csv.clients;", ttl="10m")
produit = conn.query("SELECT * FROM csv.produits_sous_categorie;", ttl="10m")
ventes = conn.query("SELECT * FROM csv.ventes;", ttl="10m")

client = client.groupby(["sex"]).count()
client = client.drop("client_id", axis=1).reset_index()

produit = produit.groupby(["category"]).count().reset_index()

ventes["date"] = pd.to_datetime(ventes["date"], dayfirst=True)
ventes["date"] = ventes["date"].dt.strftime("%m")
ventes = ventes.groupby(["date"]).count().reset_index()

fig1 = px.pie(client, values="birth", names="sex")
fig2 = px.bar(produit, x="category", y="price")
fig3 = px.bar(ventes, x="date", y="client_id")

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.plotly_chart(fig2, use_container_width=True)

st.plotly_chart(fig3, use_container_width=True)


st.write(client)
st.write(produit)
st.write(ventes)
