import mysql.connector
from mysql.connector import Error

def prepare_odb():
    # Connect to MySQL database
    conn = None
    create_db = " CREATE DATABASE odb"
    use_db = "use odb"
    create_region = "CREATE TABLE region (region_id INT NOT NULL PRIMARY KEY, " \
                     "region VARCHAR(100))" 
    create_country = "CREATE TABLE country (country_id INT NOT NULL PRIMARY KEY, " \
                     "country VARCHAR(100), region_id INT, FOREIGN KEY (region_id) REFERENCES region(region_id))"   
    create_provstate = "CREATE TABLE provstate (provstate_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "provstate VARCHAR(100), country_id INT, FOREIGN KEY (country_id) REFERENCES country(country_id))" 
    create_city = "CREATE TABLE city (city_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "city VARCHAR(100), provstate_id INT, FOREIGN KEY (provstate_id) REFERENCES provstate(provstate_id))"   
    create_target = "CREATE TABLE target (target_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "target VARCHAR(400), targettype_id INT, targettype VARCHAR(100))"
    create_event = "CREATE TABLE event (event_id BIGINT NOT NULL PRIMARY KEY, " \
                     "year INT, month INT, day INT, crit1 INT, crit2 INT, crit3 INT, success INT, suicide INT, attacktype_id INT, attacktype VARCHAR(100), gname VARCHAR(400), " \
                     "individual INT, nkill INT, nwound INT, property INT, city_id INT, provstate_id INT, country_id INT, region_id INT, target_id INT, " \
                     "FOREIGN KEY (city_id) REFERENCES city(city_id), FOREIGN KEY (provstate_id) REFERENCES provstate(provstate_id), FOREIGN KEY (region_id) REFERENCES region(region_id), " \
                     "FOREIGN KEY (country_id) REFERENCES country(country_id), FOREIGN KEY (target_id) REFERENCES target(target_id))"

    try:
        conn = mysql.connector.connect(host='localhost', 
                                  port=13306, 
                                  user='dbuser',
                                  password='secret')
        if conn.is_connected():
                print('Connected to MySQL database')
    
        cursor = conn.cursor()
        cursor.execute(create_db)
        cursor.execute(use_db)
        cursor.execute(create_region)
        cursor.execute(create_country)
        cursor.execute(create_provstate)
        cursor.execute(create_city)
        cursor.execute(create_target)
        cursor.execute(create_event)
        
        conn.commit()

        print('ODB is prepared')
        
            
    except Error as e:
        print(e)
        
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()

def alter_odb():
    # to add foreign keys
    conn = None
    use_db = "use odb"
    alter_region = "ALTER TABLE region ADD FOREIGN KEY (country) REFERENCES country(country)"
    alter_provstate = "ALTER TABLE provstate ADD FOREIGN KEY (region) REFERENCES region(region)"
    alter_city = "ALTER TABLE city ADD FOREIGN KEY (provstate_id) REFERENCES provstate(provstate_id)"
    alter_event = "ALTER TABLE event ADD FOREIGN KEY (country) REFERENCES country(country)"
    alter_event2 = "ALTER TABLE event ADD FOREIGN KEY (region) REFERENCES region(region)"
    alter_event3 = "ALTER TABLE event ADD FOREIGN KEY (provstate_id) REFERENCES provstate(provstate_id)"
    alter_event4 = "ALTER TABLE event ADD FOREIGN KEY (city_id) REFERENCES city(city_id)"

    try:
        conn = mysql.connector.connect(host='localhost', 
                                  port=13306, 
                                  user='dbuser',
                                  password='secret')
        if conn.is_connected():
                print('Connected to MySQL database')
    
        cursor = conn.cursor()
        cursor.execute(use_db)
        cursor.execute(alter_region)
        cursor.execute(alter_provstate)
        cursor.execute(alter_city)
        cursor.execute(alter_event)
        cursor.execute(alter_event2)
        cursor.execute(alter_event3)
        cursor.execute(alter_event4)
        
        conn.commit()

        print('ODB is altered')
    
    except Error as e:
        print(e)
    
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()

def prepare_dw():
    # Connect to MySQL database
    conn = None
    create_db = " CREATE DATABASE dw"
    use_db = "use dw"
    create_table = "CREATE TABLE fact (fact_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "event_id BIGINT, year INT, month INT, day INT, crit1 INT, crit2 INT, crit3 INT, success INT, suicide INT, attacktype_id INT, attacktype VARCHAR(100), gname VARCHAR(400), " \
                     "individual INT, nkill INT, nwound INT, property INT, country_id INT, region_id INT, provstate_id INT, city_id INT) "
    try:  
        conn = mysql.connector.connect(host='localhost',
                                  port=23306, 
                                  user='dbuser',
                                  password='secret')
        if conn.is_connected():
                print('Connected to MySQL database')
        
        cursor = conn.cursor()
        cursor.execute(create_db)
        cursor.execute(use_db)
        cursor.execute(create_table)
        
        conn.commit()

        print('DW is prepared')
        
            
    except Error as e:
        print(e)
        
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()
            
if __name__ == '__main__':
    prepare_odb()
    #alter_odb()
    prepare_dw()
    