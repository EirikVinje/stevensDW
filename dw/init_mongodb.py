from pymongo import MongoClient
import polars as pl
import json
import sys
import multiprocessing
import threading
import time
import numpy as np
import pycountry_convert



class TerroristMongoDBDatabase:

    def __init__(self, path, columns : list[str] = None):

        self.raw = pl.read_csv(path, infer_schema_length=0)

        self.raw = self.raw.with_columns(
            pl.col("event_id").cast(pl.Int64),
            pl.col("year").cast(pl.Int64),
            pl.col("month").cast(pl.Int64),
            pl.col("day").cast(pl.Int64),
            pl.col("country_id").cast(pl.Int64),
            pl.col("country").cast(pl.String),
            pl.col("region_id").cast(pl.Int64),
            pl.col("region").cast(pl.String),
            pl.col("provstate").cast(pl.String),
            pl.col("city").cast(pl.String),
            pl.col("crit1").cast(pl.Int64),
            pl.col("crit2").cast(pl.Int64),
            pl.col("crit3").cast(pl.Int64),
            pl.col("doubtterr").cast(pl.Int64),
            pl.col("success").cast(pl.Int64),
            pl.col("suicide").cast(pl.Int64),
            pl.col("attacktype_id").cast(pl.Int64),
            pl.col("attacktype").cast(pl.String),
            pl.col("targettype_id").cast(pl.Int64),
            pl.col("targettype").cast(pl.String),
            pl.col("target").cast(pl.String),
            pl.col("gname").cast(pl.String),
            pl.col("individual").cast(pl.Int64),
            pl.col("nkill").cast(pl.Float64),
            pl.col("nwound").cast(pl.Float64),
            pl.col("property").cast(pl.Int64)
        )

        self.columns = None
        with open("columns.json", "rb") as f:
            self.columns = json.load(f)["columns"]

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        try:
            db.validate_collection("events")
        except:
            print("Events collection not found, creating...")
            self._create_collection("events")
            self._insert_all_events()

        try:
            db.validate_collection("countries")
        except:
            print("Countries collection not found, creating...")
            self._create_collection("countries")
            self._insert_countries()



    def _create_collection(self, collection_name : str):

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        if collection_name in db.list_collection_names():
            print(f"Collection {collection_name} already exists")
            return

        db.create_collection(collection_name)
        
        client.close()
            
    

    def _insert_countries(self):

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        # extract country and country text, only unique by country
        countries = self.raw.unique(subset=["country"])

        # get all provstates, regions and cities for each country and add as lists to the country in the collection 

        

        data_dict = {}

        for i in range(self.raw.shape[0]):
            row = self.raw[i]

            if row["country_id"][0] in data_dict:
                # print('yo')
                if row["provstate"][0] not in data_dict[row["country_id"][0]]["provstate"]:
                    data_dict[row["country_id"][0]]["provstate"].append(row["provstate"][0])
                if row["region"][0] not in data_dict[row["country_id"][0]]["region"]:
                    data_dict[row["country_id"][0]]["region"].append(row["region"][0])
                if row["city"][0] not in data_dict[row["country_id"][0]]["city"]:
                    data_dict[row["country_id"][0]]["city"].append(row["city"][0])

            else:
                data_dict[row["country_id"][0]] = {"countryid": row["country_id"][0],
                                        "country": row["country"][0],
                                        "provstate": [row["provstate"][0]],
                                        "region": [row["region"][0]],
                                        "city": [row["city"][0]]}

        # print(data_dict)

        db["countries"].insert_many(data_dict.values())


        client.close()

    def _insert_all_events(self):
        # insert all events with multithreading

        queries = []

        for i in range(self.raw.shape[0]):
            row = self.raw[i]

            data_dict = {}
            for key in self.raw.columns:
                data_dict[key] = row[key][0]

            queries.append(data_dict)

        # print(len(queries))


        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        db["events"].insert_many(queries)
        client.close



    def get_num_events_all_countries(self):
        

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]


        result = db["events"].aggregate(
            [{
                "$group": {
                    "_id": "$country",
                    "count": {"$sum": 1}
                }
            }]
            )

        data = []
        for i in result:
            try:
                country = pycountry_convert.country_name_to_country_alpha3(i["_id"])
            except:
                continue
            if len(country) == 3:
                data.append([country, i["count"], i["_id"]])
            else:
                pass

        df = pl.DataFrame(data, schema=["iso_alpha", "count", "country"])
        client.close()

        return df

    def get_events_by_country(self, country):
        """
        Get number of events for each year for a specific country

        Format:
        year | num_events | nkill
        """

        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        result = db["events"].aggregate(
            [
                {
                    "$match": {
                        "country": country
                    }
                },
                {
                    "$group": {
                        "_id": "$year",
                        "num_events": {"$sum": 1},
                        "nkill": {"$sum": "$nkill"}
                    }
                }
            ]
        )

        data = []
        for i in result:
            data.append([i["_id"], i["num_events"], i["nkill"]])

        df = pl.DataFrame(data, schema=["year", "num_events", "nkill"])

        return df

    def get_events_with_criteria(self, country=None, start_year=None, end_year=None, attack_type=None, target_type=None, success=None):
        
        client = MongoClient("mongodb://localhost:27017")
        db = client["mongodb_database"]

        if start_year != None and end_year == None:
            end_year = start_year


        query = {"country": country, 
                 "year": {"$gte": start_year, "$lte": end_year},
                 "attacktype": attack_type, 
                 "targettype": target_type, 
                 "success": success}

        # remove None values from query
        query = {k: v for k, v in query.items() if v is not None}
        if "year" in query and query["year"] == {"$gte": None, "$lte": None}:
            del query["year"]

        # extract only these columns: year, month, day, region, country, provstate, city, target, targettype, success, suicide, attacktype, gname, nkill, nwound, individual, property
        result = list(db["events"].find(query))
        df = pl.DataFrame(result)
        df = df.select(["year", "month", "day", "region", "country", "provstate", "city", "target","targettype","success","suicide","attacktype","gname","nkill","nwound","individual","property"])

        # df = df.drop(["_id", "country_id", "region_id", "attacktype_id", "targettype_id"])
        print(df.columns)
        # extract only these columns: year, month, day, region, country, provstate, city, target, targettype, success, suicide, attacktype, gname, nkill, nwound, individual, property
        
        return df







if __name__ == "__main__":

    path = "../data/terrorismdb_no_doubt.csv"

    database = TerroristMongoDBDatabase(path)
    
    args = sys.argv[-1]
    
    print(database.get_events_with_criteria("United States", 2001, 2001))

    
