import pymongo
import json

client = pymongo.MongoClient("mongodb://mongoadmin:mongoadmin@localhost:27017/")

db = client["aisthair"]


for i in ["clients", "produits_sous-categorie", "ventes"]:

    Collection = db[i]

    with open(f"data/json/{i}.json") as file:
        file_data = json.load(file)

    if isinstance(file_data, list):
        Collection.insert_many(file_data)
    else:
        Collection.insert_one(file_data)
