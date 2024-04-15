import mysql.connector
from mysql.connector import Error

def prepare_odb():
    # Connect to MySQL database
    conn = None
    create_db = " CREATE DATABASE odb"
    use_db = "use odb"
    create_region = "CREATE TABLE region (regionID INT NOT NULL PRIMARY KEY, " \
                     "region_txt VARCHAR(100))" 
    create_country = "CREATE TABLE country (countryID INT NOT NULL PRIMARY KEY, " \
                     "country_txt VARCHAR(100), regionID INT, FOREIGN KEY (regionID) REFERENCES region(regionID))"   
    create_provstate = "CREATE TABLE provstate (provstateID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "provstate VARCHAR(100), countryID INT, FOREIGN KEY (countryID) REFERENCES country(countryID))" 
    create_city = "CREATE TABLE city (cityID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "city VARCHAR(100), provstateID INT, FOREIGN KEY (provstateID) REFERENCES provstate(provstateID))"   
    create_target = "CREATE TABLE target (targetID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "target VARCHAR(100), targtype INT, targtype_txt VARCHAR(100))"
    create_event = "CREATE TABLE event (eventID INT NOT NULL PRIMARY KEY, " \
                     "iyear INT, imonth INT, iday INT, crit1 INT, crit2 INT, crit3 INT, success INT, suicide INT, gname VARCHAR(100), " \
                     "individual INT, nkill INT, nwound INT, property INT, cityID INT, provstateID INT, regionID INT, countryID INT, targetID INT, " \
                     "FOREIGN KEY (cityID) REFERENCES city(cityID), FOREIGN KEY (provstateID) REFERENCES provstate(provstateID), FOREIGN KEY (regionID) REFERENCES region(regionID), " \
                     "FOREIGN KEY (countryID) REFERENCES country(countryID), FOREIGN KEY (targetID) REFERENCES target(targetID))"

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
    alter_city = "ALTER TABLE city ADD FOREIGN KEY (provstateId) REFERENCES provstate(provstateID)"
    alter_event = "ALTER TABLE event ADD FOREIGN KEY (country) REFERENCES country(country)"
    alter_event2 = "ALTER TABLE event ADD FOREIGN KEY (region) REFERENCES region(region)"
    alter_event3 = "ALTER TABLE event ADD FOREIGN KEY (provstateID) REFERENCES provstate(provstateID)"
    alter_event4 = "ALTER TABLE event ADD FOREIGN KEY (cityID) REFERENCES city(cityID)"

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
    create_table = "CREATE TABLE fact (factId INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "eventID INT, iyear INT, imonth INT, iday INT, crit1 INT, crit2 INT, crit3 INT, success INT, suicide INT, gname VARCHAR(100), " \
                     "individual INT, nkill INT, nwound INT, property INT, countryID INT, regionID INT, provstateID INT, cityID INT) "
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
    