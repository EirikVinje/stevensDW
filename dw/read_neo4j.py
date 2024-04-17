from neo4j import GraphDatabase
import pycountry_convert
import polars as pl
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

        query = "Match (e:Event {}) Return e.day, e.month, e.year".format("{country : 'Iraq'}")
        # query = input("Enter query: ")

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
                RETURN c.country AS country, count(e) AS count
                ORDER BY count DESC
                """

            res2 = session.run(q)

            df = []
            for record in res2:
                
                r = record.data()

                a3 = self._get_alpha_3(r["country"])

                if a3 is None:
                    continue
                    
                r["iso_alpha"] = a3

                df.append(r)

            df = pl.DataFrame(df)

            print(df)

            end_t = time.time()
            
            print(f"Time to run query: {end_t - start_t}")

            return df


    def get_events_by_country(self, country : str):
        
        start_t = time.time()

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            query = f"""
                    MATCH (c:Country)-[:HAS_EVENT]->(e:Event)
                    WHERE c.country = '{country}'
                    RETURN e.year AS year, sum(e.nkill) AS nkill, count(e) as num_events 
                    ORDER BY year
                    """
            
            result = session.run(query)
        
            df = pl.DataFrame([r.data() for r in result])

            print(df)

            end_t = time.time()

            print(f"Time to run query: {end_t - start_t}")

            return df
        

    def get_events_with_criteria(self, country : str, start_year : int, end_year : int, attacktype : str, targettype : str, success : int):
        
        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            start_t = time.time()

            query = f"""
                    MATCH (e:Event)
                    WHERE e.country = '{country}' AND e.year >= {start_year} AND e.year <= {end_year}
                    AND e.attacktype = '{attacktype}' AND e.targettype = '{targettype}' AND e.success = {success}
                    RETURN e.year as year, 
                           e.month as month, 
                           e.day as day, 
                           e.region as region, 
                           e.country as country, 
                           e.provstate as provstate, 
                           e.city as city, 
                           e.target as target, 
                           e.targettype as targettype, 
                           e.success as success, 
                           e.suicide as suicide, 
                           e.attacktype as attacktype, 
                           e.gname as gname, 
                           e.nkill as nkill, 
                           e.nwound as nwound, 
                           e.individual as individual, 
                           e.property as property
                    """
            
            result = session.run(query)

            df = pl.DataFrame([r.data() for r in result])

            print(df)    

            end_t = time.time()

            print(f"Time to run query: {end_t - start_t}")

            return df


if __name__ == "__main__":

    db = TerroristNeo4JDatabase()
    
    db.get_num_events_all_countries()
    # db.get_events_by_country("Iraq")
    # db.custom_query()
    # db.get_events_with_criteria(country="Norway", start_year=2000, end_year=2020, attacktype=2, targettype=14, success=1)