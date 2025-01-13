from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.schema import CreateSchema
import pandas as pd

clients_csv = pd.read_csv(
    "data/clients.csv",
    delimiter=";",
)

ventes_csv = pd.read_csv(
    "data/ventes.csv",
    delimiter=";",
)
produits_sous_categorie_csv = pd.read_csv(
    "data/produits_sous_categorie.csv",
    delimiter=";",
)

table_names = ["clients", "ventes", "produits_sous_categorie"]
tables_csv = [clients_csv, ventes_csv, produits_sous_categorie_csv]

engine = create_engine(f"postgresql://hihi:houhou@localhost:5432/postgres")

if not database_exists(engine.url):
    create_database(engine.url)

with engine.connect() as conn:

    conn.execute(CreateSchema("csv", if_not_exists=True))
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


for table_name, table_csv in zip(table_names, tables_csv):
    metadata_csv = MetaData(schema="csv")

    columns_csv = [
        Column(name, infer_sqlalchemy_type(dtype))
        for name, dtype in table_csv.dtypes.items()
    ]

    create_table_csv = Table(table_name, metadata_csv, *columns_csv)

    create_table_csv.create(engine)
    conn.commit()

    table_csv.to_sql(
        table_name, schema="csv", con=engine, if_exists="append", index=False
    )
