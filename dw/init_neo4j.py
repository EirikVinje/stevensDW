import json
import sys

from neo4j import GraphDatabase
from tqdm import tqdm
import polars as pl
import concurrent.futures as cf


class TerroristNeo4JDatabase:

    def __init__(self, path):

        self.raw = pl.read_csv(path, infer_schema_length=0)
        
        self.uri = "bolt://localhost:7687"
        self.username = "neo4j"
        self.password = "password"

        self.columns = None
        with open("columns.json", "rb") as f:
            self.columns = json.load(f)["columns"]

        self.raw = self.raw[self.columns]

        self.max_reads = 5000

        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        

    def insert_country_nodes(self):

        columns = ["country", "country_txt"]

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            for i in tqdm(range(self.raw.shape[0])):
                
                if i == self.max_reads:
                    break
                
                row = self.raw[i]

                query = self._create_query("Country", columns, row)

                _q = "{" + f"country : '{row['country'][0]}'" + "}"
                query_exists = f"MATCH (n:Country {_q}) return n"

                res = session.run(query_exists) 

                if res.peek() is None:
                    session.run(query)

                

        driver.close()
    

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


    def insert_events(self):

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        columns = [
            "eventid",
            "iyear",
            "imonth",
            "iday",
            "country",
            "country_txt",
            "region",
            "region_txt",
            "provstate",
            "city",
            "crit1",
            "crit2",
            "crit3",
            "doubtterr",
            "success",
            "suicide",
            "attacktype1",
            "attacktype1_txt",
            "targtype1",
            "targtype1_txt",
            "target1",
            "gname",
            "individual",
            "nkill",
            "nwound",
            "property"
            ]

        with driver.session() as session:
            
            for i in tqdm(range(self.raw.shape[0])):
                
                if i == self.max_reads:
                    break

                query = self._create_query("Event", columns, self.raw[i])

                session.run(query)

        driver.close()


    def create_relationships(self):

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            country_ids = session.run("MATCH (c:Country) RETURN c.country").values()

            for idx in tqdm(country_ids):    

                query = self._match_query("Country", "Event", "country", "country", "HAS_EVENT", idx[0])
                session.run(query)
                
        driver.close()


    def delete_all(self):

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            query = "MATCH (n) DETACH DELETE n"
            session.run(query)

        driver.close()
        

    def insert_one(self, session, row : pl.DataFrame, countries : list, events : list):

        if int(row["country"][0]) not in countries:
            countries.append(int(row["country"][0]))
            query = self._create_query("Country", ["country", "country_txt"], row)
            session.run(query)

        if int(row["eventid"][0]) not in events:
            events.append(int(row["eventid"][0]))
            query = self._create_query("Event", ["eventid", "iyear", "country", "country_txt", "region", "region_txt", "provstate", "city", "crit1", "crit2", "crit3", "doubtterr", "success", "suicide", "attacktype1", "attacktype1_txt", "targtype1", "targtype1_txt", "target1", "gname", "individual", "nkill", "nwound", "property"], row)
            session.run(query)
            

    def insert_all(self):

        countries = []
        events = []
        with self.driver.session() as session:
            
            with cf.ThreadPoolExecutor(max_workers=10) as executor:
                
                for i in tqdm(range(self.raw.shape[0])):
                    
                    if i == self.max_reads:
                        break
                    
                    executor.submit(self.insert_one, session, self.raw[i], countries, events)
                
        self.driver.close()


if __name__ == "__main__":

    path = "/home/eirik/projects/stevensDW/data/globalterrorismdb_0522dist.csv"
    db = TerroristNeo4JDatabase(path)
    
    args = sys.argv[-1]

    if args == "ic":
        db.insert_country_nodes()

    elif args == "ie":
        db.insert_events()

    elif args == "d":
        db.delete_all()
    
    elif args == "ir":
        db.create_relationships()

    elif args == "ia":
        db.insert_all()