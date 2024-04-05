import polars as pl
from neo4j import GraphDatabase


class TerroristNeo4JDatabase:

    def __init__(self, path, columns : list = None):

        self.raw = pl.read_csv(path, infer_schema_length=0)
        self.raw = self.raw.select(columns)    
    
        self.uri = "bolt://localhost:7687"
        self.username = "neo4j"
        self.password = "password"
    
    def insert(self):

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            query = "CREATE (p:Person {name: 'Emma', age: 25}) RETURN p"
            session.run(query)

            query = "CREATE (p:Person {name: 'Dane', age: 67}) RETURN p"
            session.run(query)

        driver.close()


    def read(self):
        
        """
        A method to read nodes from the graph database.
        """

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            query = "MATCH (n) RETURN n"
            result = session.run(query)

            for record in result:
                print(record)

        driver.close()
        

if __name__ == "__main__":

    path = "/home/eirik/data/terrorist_dataset/globalterrorismdb_0522dist.csv"
    columns = ["eventid", "iyear", "country_txt"]
    db = TerroristNeo4JDatabase(path, columns)
    db.insert()
    db.read()
    