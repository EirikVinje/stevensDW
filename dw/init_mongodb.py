from pymongo import MongoClient
import polars as pl
import json


class TerroristMongoDBDatabase:

    def __init__(self, path, columns : list[str] = None):

        self.raw = pl.read_csv(path, infer_schema_length=0)

        self.columns = None
        with open("columns.json", "rb") as f:
            self.columns = json.load(f)["columns"]

        self.raw = self.raw[self.columns]



    def create_collection(self, collection_name : str):

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        if collection_name in db.list_collection_names():
            assert ValueError(f"Collection {collection_name} already exists")

        db.create_collection(collection_name)
        
        client.close()

    def insert(self, collection_name : str):

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        if collection_name not in db.list_collection_names():
            assert ValueError(f"Collection {collection_name} does not exist")
        
        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        data_i = self.raw[0]
        
        data_dict = {"eventid": int(data_i[0,0]),
                    "iyear": int(data_i[0,1]),
                    "country_txt": data_i[0,2]}

        db[collection_name].insert_one(data_dict)

        client.close()
            
    def select(self, collection_name : str):

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        if collection_name not in db.list_collection_names():
            assert ValueError(f"Collection {collection_name} does not exist")

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        for data_i in db[collection_name].find():
            print(data_i)


if __name__ == "__main__":

    path = "/home/eirik/data/terrorist_dataset/globalterrorismdb_0522dist.csv"

    database = TerroristMongoDBDatabase(path)
    
    # database.create_collection("country_year")
    # database.insert("country_year")
    database.select("country_year")
