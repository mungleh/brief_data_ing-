from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.schema import CreateSchema
import pandas as pd
import json

clients_csv = pd.read_csv(
    "brief_esther/Les données, alimenter une base de données/data/csv/clients.csv",
    delimiter=";",
)

ventes_csv = pd.read_csv(
    "brief_esther/Les données, alimenter une base de données/data/csv/ventes.csv",
    delimiter=";",
)
produits_sous_categorie_csv = pd.read_csv(
    "brief_esther/Les données, alimenter une base de données/data/csv/produits_sous_categorie.csv",
    delimiter=";",
)

clients_json = pd.json_normalize(
    json.load(
        open(
            "brief_esther/Les données, alimenter une base de données/data/json/clients.json"
        )
    )
)

ventes_json = pd.json_normalize(
    json.load(
        open(
            "brief_esther/Les données, alimenter une base de données/data/json/clients.json"
        )
    )
)

produits_sous_categorie_json = pd.json_normalize(
    json.load(
        open(
            "brief_esther/Les données, alimenter une base de données/data/json/clients.json"
        )
    )
)

table_names = ["clients", "ventes", "produits_sous_categorie"]
tables_csv = [clients_csv, ventes_csv, produits_sous_categorie_csv]
tables_json = [clients_json, ventes_json, produits_sous_categorie_json]

engine = create_engine(f"postgresql://hihi:houhou@localhost:5432/aisthair")

if not database_exists(engine.url):
    create_database(engine.url)

with engine.connect() as conn:

    conn.execute(CreateSchema("csv", if_not_exists=True))
    conn.execute(CreateSchema("json", if_not_exists=True))
    conn.commit()


def infer_sqlalchemy_type(dtype):
    """Map pandas dtype to SQLAlchemy's types"""
    if "int" in dtype.name:
        return Integer
    elif "float" in dtype.name:
        return Float
    elif "object" in dtype.name:
        return String(255)
    else:
        return String(255)


for table_name, table_csv, table_json in zip(table_names, tables_csv, tables_json):
    metadata_csv = MetaData(schema="csv")
    metadata_json = MetaData(schema="json")

    columns_csv = [
        Column(name, infer_sqlalchemy_type(dtype))
        for name, dtype in table_csv.dtypes.items()
    ]

    columns_json = [
        Column(name, infer_sqlalchemy_type(dtype))
        for name, dtype in table_json.dtypes.items()
    ]

    create_table_csv = Table(table_name, metadata_csv, *columns_csv)
    create_table_json = Table(table_name, metadata_json, *columns_json)

    create_table_csv.create(engine)
    create_table_json.create(engine)
    conn.commit()

    table_csv.to_sql(
        table_name, schema="csv", con=engine, if_exists="append", index=False
    )
    table_json.to_sql(
        table_name, schema="json", con=engine, if_exists="append", index=False
    )
