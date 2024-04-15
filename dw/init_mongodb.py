from pymongo import MongoClient
import polars as pl
import json
import sys

class TerroristMongoDBDatabase:

    def __init__(self, path, columns : list[str] = None):

        self.raw = pl.read_csv(path, infer_schema_length=0)

        self.columns = None
        with open("columns.json", "rb") as f:
            self.columns = json.load(f)["columns"]

        self.raw = self.raw[self.columns['columns']]



    def create_collection(self, collection_name : str):

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        if collection_name in db.list_collection_names():
            print(f"Collection {collection_name} already exists")
            return

        db.create_collection(collection_name)
        
        client.close()

    def insert(self, collection_name : str, data):

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        if collection_name not in db.list_collection_names():
            assert ValueError(f"Collection {collection_name} does not exist")
        
        
        # select columns
    


        for i in range(data.shape[0]):
            row = data[i]

            data_dict = {}
            for key in row.keys():
                data_dict[key] = row[key][0]

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

    def insert_countries(self):

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        # extract country and country text, only unique by country
        countries = self.raw.unique(subset=["country"])

        # get all provstates, regions and cities for each country and add as lists to the country in the collection 

        

        data_dict = {}

        for i in range(countries.shape[0]):
            row = countries[i]

            if row["country"][0] in data_dict:
                data_dict["country"]["provstate"].append(row["provstate"][0])
                data_dict["country"]["region"].append(row["region"][0])
                data_dict["country"]["city"].append(row["city"][0])

            else:
                data_dict["country"] = {"country": row["country"][0],
                                        "country_txt": row["country_txt"][0],
                                        "provstate": [row["provstate"][0]],
                                        "region": [row["region"][0]],
                                        "city": [row["city"][0]]}

            # data_dict["country"] = {"country": row["country"][0],
            #                         "country_txt": row["country_txt"][0]}

            
        

        # print(country_dict)
        # assert False

        db["countries"].insert_many(country_dict)

        # for i in range(self.raw.shape[0]):



        #     row = self.raw[i]

        #     data_dict = {"country": row['country'],
        #                  "country_txt": row['country_txt']}

        #     db["countries"].insert_one(data_dict)

        client.close()

    def insert_events(self):

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        # get 5 events for USA
        # events = self.raw.filter(self.raw["country_txt"] == "United States")
        # events = events.head(5)

        # get all events
        events = self.raw

        for i in range(events.shape[0]):
            row = events[i]
            
            data_dict = {"eventid": row["eventid"][0],
                        "iyear": row["iyear"][0],
                        "region": row["region"][0],
                        "region_txt": row["region_txt"][0],
                        "provstate": row["provstate"][0],
                        "country": row["country"][0]}

            # print(data_dict)


            db["events"].insert_one(data_dict)

        client.close()

    def test_query(self):
        
        # get all events for USA, get which ID is USA from country collection, and then find all events with that ID

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        # find all events and get the amount per country, get country_Txt from countries collection
        pipeline = [
            {"$lookup": {
                "from": "countries",
                "localField": "country",
                "foreignField": "country",
                "as": "country_txt"
            }},
            {"$unwind": "$country_txt"},
            {"$group": {"_id": "$country_txt.country_txt", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        

        result = db.events.aggregate(pipeline)

        for data_i in result:
            print(data_i)

        client.close()


    def test_full_setup(self):
        # create collections
        self.create_collection("country")
        self.create_collection("event")


        # insert data








if __name__ == "__main__":

    path = "../data/globalterrorismdb_0522dist.csv"

    database = TerroristMongoDBDatabase(path)
    
    args = sys.argv[-1]
    
    if args == "insert_countries":
        database.create_collection("countries")
        database.insert_countries()
    elif args == "insert_events":
        database.create_collection("events")
        database.insert_events()

    elif args == "select_countries":
        database.select("countries")
    elif args == "select_events":
        database.select("events")

    else:
        database.test_query()

    
