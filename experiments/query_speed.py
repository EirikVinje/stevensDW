import sys
sys.path.append("/Users/tobiasbrambo/projects/stevensDW/dw/")



from init_mongodb import TerroristMongoDBDatabase
from init_sql import TerroristSQLDatabase
from read_neo4j import TerroristNeo4JDatabase

from time import perf_counter

def query_speed():

    data_path = "/Users/tobiasbrambo/projects/stevensDW/data/terrorismdb_no_doubt.csv"

    mongo_times = []
    sql_times = []
    neo4j_times = []

    mongodb = TerroristMongoDBDatabase(data_path)
    sql = TerroristSQLDatabase(data_path)
    neo4j = TerroristNeo4JDatabase()

    queries = [
        {"country":"Iraq",
         "start_year":2010,
         "end_year":2010,
         "attack_type":"Bombing/Explosion"},
        {"country":"United States",
         "start_year":2001},
        {"country":"Norway",
         "start_year":2001,
         "end_year":2011,
         "attack_type":"Bombing/Explosion"}
    ]

    for query in queries:
        start = perf_counter()
        mongodb.get_events_with_criteria(**query)
        end = perf_counter()
        mongo_times.append(end-start)

        start = perf_counter()
        sql.get_events_with_criteria(**query)
        end = perf_counter()
        sql_times.append(end-start)

        start = perf_counter()
        neo4j.get_events_with_criteria(**query)
        end = perf_counter()
        neo4j_times.append(end-start)


    print(f"MongoDB: {mongo_times}")
    print(f"SQL: {sql_times}")
    print(f"Neo4J: {neo4j_times}")


if __name__ == "__main__":
    query_speed()
