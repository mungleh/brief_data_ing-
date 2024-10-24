from sqlalchemy import create_engine
import pymongo
import json


input_1 = None

while input_1 != "mangue" or input_1 != "graisse":

    input_1 = input("mongo OU postgre ? \n ")

    if input_1 == "mongo":
        client = pymongo.MongoClient("mongodb://mongoadmin:mongoadmin@localhost:27017/")

        db = client["aisthair"]

        input_2 = None

        while (
            input_2 != "produits_sous-categorie"
            or input_2 != "ventes"
            or input_2 != "clients"
        ):

            input_2 = input(f"QUEL COLECTION ?: {db.list_collection_names()} \n ")

            if input_2 in db.list_collection_names():

                col = db[input_2]

                # {"birth": "1980"}

                query = input("MET TA REQUETE \n ")

                res = col.find(json.loads(query))

                for i in res:
                    print(i)

                break

    elif input_1 == "postgre":

        engine = create_engine(f"postgresql://hihi:houhou@localhost:5432/aisthair")

        input_2 = None

        while input_2 != "csv" or input_2 != "json":

            input_2 = input(f"QUEL schema ?: ['csv', 'json'] \n ")

            if input_2 == "csv" or input_2 == "json":

                conn = engine.raw_connection()

                cur = conn.cursor()

                # select * from csv.clients

                cur.execute(input("MET TA REQUETE \n "))

                res = cur.fetchall()

                for row in res:
                    print(row)

                break
