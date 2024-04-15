import multiprocessing
import threading
import time
import json
import sys
import gc

from neo4j import GraphDatabase
from tqdm import tqdm
import polars as pl
import numpy as np

class TerroristNeo4JDatabase:

    def __init__(self, datapath, max_reads : int = 10_000):

        self.uri = "bolt://localhost:7687"
        self.username = "neo4j"
        self.password = "password"

        with open("columns.json", "rb") as f:
            self.columns = json.load(f)["columns"]

        self.raw = pl.read_csv(datapath, infer_schema_length=0)[self.columns]
        self.max_reads = max_reads
        self.n_threads = multiprocessing.cpu_count() - 1
        

    def _create_query(self, node : str, columns : list[str], row : pl.DataFrame):
        
        values = [row[c][0] for c in columns]

        querydict = "{"
        for key, value in zip(columns, values):
            
            if value is None:
                querydict += f"{key} : 'None', "

            elif value is not None:
                value = value.replace('&', 'and')
                value = value.replace("'", "")
                querydict += f"{key} : '{value}', "
        
        querydict = querydict[:-2] + "}"
        
        query = f"CREATE (n:{node} {querydict})"

        return query


    def _match_query(self, node : str, node_to, key : str, key_to : str, relationship : str, value):

        nk1 =  "(" + f"a:{node}" + " " "{" + f"{key} : '{value}'" + "}" + ")"
        nk2 =  "(" + f"b:{node_to}" + " " "{" + f"{key_to} : '{value}'" + "}" + ")"
        
        relationship = f"(a)-[:{relationship}]->(b)"

        query = f"MATCH {nk1}, {nk2} CREATE {relationship}"

        return query


    def _thread_relationships(self, queries : list[str], i : int):

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:

            for query in tqdm(queries, desc="Thread {} on {} queries".format(i, len(queries))):
                session.run(query)
        
        driver.close()


    def create_relationships(self):
        
        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        
        with driver.session() as session:
            country_ids = session.run("MATCH (c:Country) RETURN c.country").values()
        
        driver.close()

        queries = []
        for country_id in country_ids:
            query = query = self._match_query("Country", "Event", "country", "country", "HAS_EVENT", country_id[0])
            queries.append(query)

        queries_split = np.array_split(queries, self.n_threads)

        print("\nMultithreading with {} threads over {} relationships\n".format(self.n_threads, len(queries)))

        threads = [threading.Thread(target=self._thread_relationships, args=(queries_split[i], i,)) for i in range(len(queries_split))]

        start = time.time()

        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()

        end = time.time()

        threads = None
        gc.collect()

        print("\n\nDone!")
        print("Time taken with {} threads and {} relationships: {}\n\n".format(self.n_threads, len(queries), end - start))


    def delete_all(self):

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            query = "MATCH (n) DETACH DELETE n"
            session.run(query)

        driver.close()
             
    
    def insert_all_countries(self):
        
        df = self.raw[["country", "country_txt"]].unique()

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            for i in tqdm(range(df.shape[0]), desc="inserting country nodes"):
                
                if i == self.max_reads:
                    break
                
                query = self._create_query("Country", ["country", "country_txt"], df[i])
                session.run(query)

        driver.close()


    def _thread_event(self, queries : list[str], i : int):

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:

            for query in tqdm(queries, desc="Thread {} on {} queries".format(i, len(queries))):
                session.run(query)
        
        driver.close()


    def insert_all_events(self):
                    
        df = self.raw
        queries = []
        
        for i in tqdm(range(df.shape[0]), desc="Creating queries"):
            
            if i == self.max_reads:
                break
            
            query = self._create_query("Event", ["eventid", "iyear", "country", "country_txt", "region", "region_txt", "provstate", "city", "crit1", "crit2", "crit3", "doubtterr", "success", "suicide", "attacktype1", "attacktype1_txt", "targtype1", "targtype1_txt", "target1", "gname", "individual", "nkill", "nwound", "property"], df[i])
            queries.append(query)

            
        n_threads = multiprocessing.cpu_count() - 1
        queries = np.array_split(queries, n_threads)
        
        print("\nMultithreading with {} threads over {} events\n".format(n_threads, self.max_reads))

        threads = [threading.Thread(target=self._thread_event, args=(queries[i], i,)) for i in range(len(queries))]
        
        start = time.time()

        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()

        end = time.time()

        threads = None
        gc.collect()

        print("\n\nDone!")
        print("Time taken with {} threads and {} events: {}\n\n".format(n_threads, self.max_reads, end - start))
                

if __name__ == "__main__":

    datapath = "/home/eirik/projects/stevensDW/data/globalterrorismdb_0522dist.csv"
    max_reads = 300_000
    db = TerroristNeo4JDatabase(datapath, max_reads)
    
    args = sys.argv[-1]

    
    if args == "d":
        db.delete_all()
    
    elif args == "ir":
        db.create_relationships()

    elif args == "ie":
        db.insert_all_events()
    
    elif args == "ic":
        db.insert_all_countries()
    