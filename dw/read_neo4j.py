from neo4j import GraphDatabase


class TerroristNeo4JDatabase:

    def __init__(self):
        
        self.uri = "bolt://localhost:7687"
        self.username = "neo4j"
        self.password = "password"
        

    def read_custom(self):
        
        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:

            query = str(input("Query: "))
            result = session.run(query)

            for record in result:
                print(record)

        driver.close()


    def read_event(self, node : str, key : str, value):

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

        with driver.session() as session:
            
            if isinstance(value, int):
                value = value
            
            elif isinstance(value, str):
                value = f"'{value}'"

            query = "MATCH (n:{} {}) RETURN n".format(node, "{" + f"{key} : {value}" + "}")

            result = session.run(query)

            i = 0
            for record in result:
                i += 1

            print("Number of records: ", i)

        driver.close()



if __name__ == "__main__":

    db = TerroristNeo4JDatabase()
    # db.read()
    db.read_event("Event", "country", "Ireland")