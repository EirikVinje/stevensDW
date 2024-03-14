from sqlalchemy import create_engine
from sqlalchemy import text

DB_USER = 'root'
DB_PASSWORD = 'secret'
DB_HOST = 'localhost' 
DB_NAME = 'myqsl_database'

def create_table():

    print("Creating table...")

    engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    
    with engine.connect() as connection:

        q1 = text("CREATE TABLE IF NOT EXISTS my_table (id INT, name VARCHAR(255))")
        
        connection.execute(q1)
        
        connection.commit()


def insert_data():

    print("Inserting data...")

    engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    
    with engine.connect() as connection:
        
        names = ["John", "Jane", "Bob", "Alice"]

        for i, name in enumerate(names):
            qi = text(f"INSERT INTO my_table VALUES ({i}, '{name}')")
            connection.execute(qi)

        connection.commit()


def delete_table():

    print("Deleting table...")

    engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    
    with engine.connect() as connection:

        q1 = text("DROP TABLE my_table")
        connection.execute(q1)

        connection.commit()


def select_table():

    print("Selecting table...")

    engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    
    with engine.connect() as connection:

        q1 = text("SELECT * FROM my_table")
        result = connection.execute(q1)

        for row in result:
            print(row)




if __name__ == '__main__':
    
    delete_table()
    create_table()
    insert_data()
    select_table()