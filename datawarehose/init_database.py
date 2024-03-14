# from typing import List
import polars as pl
from sqlalchemy import create_engine
from sqlalchemy import text

DB_USER = 'root'
DB_PASSWORD = 'secret'
DB_HOST = 'localhost' 
DB_NAME = 'myqsl_database'

class Terrorist_database:

    def __init__(self, path, columns : list = None):

        self.raw = pl.read_csv(path, infer_schema_length=0)
        self.raw = self.raw.select(columns)    
    
        self.engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    
    
    def select_column(self, columns : list = None):

        if columns is None:
            assert ValueError("No columns given")
        else:
            return self.raw[columns]

    
    def _make_meta_table(self, table_name, columns : list[tuple[str, str]]):

        with self.engine.connect() as connection:

            q = text(f"CREATE TABLE IF NOT EXISTS {table_name}_meta (column_name VARCHAR(255))")
            connection.execute(q)

            for value in columns:
                q = text(f"INSERT INTO {table_name}_meta VALUES ('{value[0]}')")
                connection.execute(q)

            connection.commit()


    def drop_table(self, table_name : str):

        with self.engine.connect() as connection:
            
            q = text(f"DROP TABLE {table_name}")
            connection.execute(q)
            
            connection.commit()


    def make_table(self, table_name : str, columns : list[tuple[str, str]]):

        with self.engine.connect() as connection:
            
            table_columns =  "("
            for value in columns:
                table_columns += f"{value[0]} {value[1]}, "
            table_columns = table_columns[:-2] + ")"

            q = text(f"CREATE TABLE IF NOT EXISTS {table_name} {table_columns}")
            
            connection.execute(q)
            
            connection.commit()

        # self._make_meta_table(table_name, columns)


    def insert_data(self, table_name : str):

        with self.engine.connect() as connection:
        
            for i in range(10):
                
                data_i = self.raw[i]
                q = text(f"INSERT INTO {table_name} VALUES ({i}, {int(data_i[0,1])}, '{data_i[0,2]}')")
                connection.execute(q)
        
            connection.commit()


    def read_data(self, table_name : str):

        with self.engine.connect() as connection:

            q = text(f"SELECT * FROM {table_name}")
            result = connection.execute(q)

            for row in result:
                print(row)

if __name__ == "__main__":

    path = "/home/eirik/data/globalterrorismdb_0522dist.csv"

    database = Terrorist_database(path, ["eventid", "iyear", "country_txt"])

    columns = [("eventid", "INT"), ("iyear", "INT"), ("country_txt", "VARCHAR(255)")]
    table_name = "country_year"

    database.drop_table(table_name)

    database.make_table(table_name, columns)

    database.insert_data(table_name)

    database.read_data(table_name)