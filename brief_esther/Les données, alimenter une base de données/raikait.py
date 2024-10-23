from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.schema import CreateSchema
import pandas as pd
import pymongo
import json


truc = "erh"

while truc != "mangue" or truc != "graisse":

    truc = input("MANGUE OU GRAISSE ? \n ")

    if truc == "mangue":
        client = pymongo.MongoClient("mongodb://mongoadmin:mongoadmin@localhost:27017/")

        db = client["aisthair"]

        trac = input(f"QUEL COLECTION ?: {db.list_collection_names()} \n ")

        while (
            trac != "produits_sous-categorie" or trac != "ventes" or trac != "clients"
        ):

            if trac in db.list_collection_names():

                col = db[trac]

                # {"birth": 1980}

                query = input("MET TA REKAIT \n ")

                res = col.find(json.loads(query))

                for i in res:
                    print(i)

                break

            else:
                print(" \n SAMARCHPO PELOW \n ")

    elif truc == "graisse":

        engine = create_engine(f"postgresql://hihi:houhou@localhost:5432/aisthair")

        trac = input(f"QUEL CHAIMA ?: ['csv', 'json'] \n ")

        while trac != "csv" or trac != "json":

            if trac == "csv" or trac == "json":

                conn = engine.raw_connection()

                cur = conn.cursor()

                cur.execute(input("SKOA TA REKAIT LA \n "))

                res = cur.fetchall()

                for row in res:
                    print(row)

                break

            else:
                print(" \n SAMARCHPO PELOW \n ")

print(" \n SAMARCHPO PELOW \n ")

truc = "erh"
