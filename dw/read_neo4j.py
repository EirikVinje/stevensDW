from neo4j import GraphDatabase
import polars as pl
import pycountry_convert
from tqdm import tqdm
import time


class TerroristNeo4JDatabase:

    def __init__(self):
        
        self.uri = "bolt://localhost:7687"
        self.username = "neo4j"
        self.password = "password"
        

    def _get_alpha_3(self, country : str):

        try:
            c = pycountry_convert.country_name_to_country_alpha3(country)
            return c
        
        except:
            return None


    def custom_query(self):

        query = input("QUERY: ")

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            result = session.run(query)
            
            for res in result:
                print(res.data())

        driver.close()


    def get_num_events_all_countries(self):
        
        start_t = time.time()

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:

            res1 = session.run("MATCH (c:Country) RETURN c.country")

            countries = []
            for record in res1:
                countries.append(record.data()["c.country"])
            
            q = """
                MATCH (c:Country)-[:HAS_EVENT]->(e:Event)
                RETURN c.country AS country, count(e) AS count_c
                ORDER BY count_c DESC
                """

            res2 = session.run(q)

            df = []
            for record in res2:
                
                a3 = self._get_alpha_3(record.data()["country"])

                if a3 is None:
                    continue

                c_i = {"iso_alpha" : a3,
                       "count" : record.data()["count_c"],
                       "country" : record.data()["country"]}

                df.append(c_i)

            df = pl.DataFrame(df)

            end_t = time.time()
            
            print(f"Time to run query: {end_t - start_t}")

        driver.close()

        return df


    def get_events_by_country(self, value : str):
        
        start_t = time.time()

        node = "Event"
        key = "country"

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            if isinstance(value, int):
                value = value
            
            elif isinstance(value, str):
                value = f"'{value}'"

            query = "MATCH (e:{} {}) RETURN e".format(node, "{" + f"{key} : {value}" + "}")

            result = session.run(query)
            
            df = []

            for r in result:
                r = r.data()["e"]
                a3 = self._get_alpha_3(r["country"])

                if a3 is None:
                    return None

                r["iso_alpha"] = a3 
                df.append(r)

            df = pl.DataFrame(df)

            end_t = time.time()

        print(f"Time to run query: {end_t - start_t}")

        driver.close()        

        return df


if __name__ == "__main__":

    db = TerroristNeo4JDatabase()
    
    # db.get_num_events_all_countries()
    # db.get_events_by_country("Iraq")
    # db.custom_query()