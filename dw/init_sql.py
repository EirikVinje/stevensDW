import json
import mysql.connector
import polars as pl
import numpy as np
from tqdm import tqdm
from mysql.connector import Error

class TerroristSQLDatabase:
    def __init__(self, path):

        DB_USER = 'dbuser'
        DB_PASSWORD = 'secret'
        DB_HOST = 'localhost' 
        DB_NAME = 'mysql-server'

        self.raw = pl.read_csv(path, infer_schema_length=0)

        self.raw = self.raw.with_columns(
            pl.col("event_id").cast(pl.Int64),
            pl.col("year").cast(pl.Int64),
            pl.col("month").cast(pl.Int64),
            pl.col("day").cast(pl.Int64),
            pl.col("country_id").cast(pl.Int64),
            pl.col("country").cast(pl.String),
            pl.col("region_id").cast(pl.Int64),
            pl.col("region").cast(pl.String),
            pl.col("provstate").cast(pl.String),
            pl.col("city").cast(pl.String),
            pl.col("crit1").cast(pl.Int64),
            pl.col("crit2").cast(pl.Int64),
            pl.col("crit3").cast(pl.Int64),
            pl.col("doubtterr").cast(pl.Int64),
            pl.col("success").cast(pl.Int64),
            pl.col("suicide").cast(pl.Int64),
            pl.col("attacktype_id").cast(pl.Int64),
            pl.col("attacktype").cast(pl.String),
            pl.col("targettype_id").cast(pl.Int64),
            pl.col("targettype").cast(pl.String),
            pl.col("target").cast(pl.String),
            pl.col("gname").cast(pl.String),
            pl.col("individual").cast(pl.Int64),
            pl.col("nkill").cast(pl.Float64),
            pl.col("nwound").cast(pl.Float64),
            pl.col("property").cast(pl.Int64)
        )

    def drop_table(self, table_name : str):
        conn = None
        drop_query = f"DROP TABLE {table_name}"

        try:
            conn = mysql.connector.connect(host='localhost', 
                                    port=13306, 
                                    user='dbuser',
                                    password='secret')
            if conn.is_connected():
                    print('Connected to MySQL database')
        
            cursor = conn.cursor()
            cursor.execute("use db")
            cursor.execute(drop_query)
        
        except Error as e:
            print(e)
            
        finally:
            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()

    def read_data(self, table_name : str):

        conn = None
        read_query = f"SELECT * FROM {table_name}"

        try:
            conn = mysql.connector.connect(host='localhost', 
                                    port=13306, 
                                    user='dbuser',
                                    password='secret')
            if conn.is_connected():
                    print('Connected to MySQL database')
        
            cursor = conn.cursor()
            cursor.execute("use db")
            res = cursor.execute(read_query)
            for row in res:
                print(row)
        
        except Error as e:
            print(e)
            
        finally:
            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()
    
    def prepare_db(self):
        # Connect to MySQL database
        conn = None
        create_db = " CREATE DATABASE db"
        use_db = "use db"
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
    
    def populate_odb(input_data):
        # Connect to MySQL database
        conn = None

        query_region = "INSERT INTO region(region_id,region) " \
                "VALUES(%s,%s)"
        #tuples_region = [(1,'Oslo Fylke', 1),(2,'Eastern US', 2)]    
        tuples_region = []


        query_country = "INSERT INTO country(country_id,country, region_id) " \
                "VALUES(%s,%s,%s)"
        #tuples_country = [(1,'Norway'),(2,'USA')]
        tuples_country = []      
        

        query_provstate = "INSERT INTO provstate(provstate_id,provstate, country_id) " \
                "VALUES(%s,%s,%s)"
        #tuples_provstate = [(1,'Oslo Kommune', 1),(2,'New York', 2)]
        tuples_provstate = []

        query_city = "INSERT INTO city(city_id,city, provstate_id) " \
                "VALUES(%s,%s,%s)"
        #tuples_city = [(1,'Oslo', 1),(2,'New York City', 2)]
        tuples_city = []

        query_target = "INSERT INTO target(target_id,target, targettype_id, targettype) " \
                "VALUES(%s,%s,%s,%s)"
        tuples_target = []

        query_event = "INSERT INTO event(event_id, year, month, day, crit1, crit2, crit3, success, suicide, attacktype_id, attacktype, " \
                "gname, individual, nkill, nwound, property, city_id, provstate_id, country_id, region_id, target_id)" \
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        tuples_event = []

        provstateids = {}
        cityids = {}
        targetids = {}
        
        for i in tqdm(range(len(input_data))):
            regionid = input_data['region_id'][i]
            region = input_data['region'][i]
            if (regionid,region) not in tuples_region:
                tuples_region.append((regionid,region))

            countryid = input_data['country_id'][i]
            country = input_data['country'][i]
            if (countryid,country,regionid) not in tuples_country:
                tuples_country.append((countryid,country,regionid))  
            
            provstate = input_data['provstate'][i]
            try:
                if provstateids[f'{provstate},{countryid}'] != None:
                    provstateid = provstateids[f'{provstate},{countryid}']
            except:
                provstateid = len(provstateids)+1
                provstateids[f'{provstate},{countryid}'] = provstateid
                tuples_provstate.append((provstateid,provstate,countryid))

            #if (provstateid, provstate,countryid) not in tuples_provstate:

            city = input_data['city'][i]
            #provstateid = provstateids[f'{provstate},{countryid}']
            try:
                if cityids[f'{provstate},{city}'] != None:
                    cityid = cityids[f'{provstate},{city}']
            except:
                cityid = len(cityids)+1
                cityids[f'{provstate},{city}'] = cityid
                tuples_city.append((cityid,city,provstateid))
            
            #if (city,provstateid) not in tuples_city:
            #   cityids[f'{provstate},{city}'] = len(cityids)+1
            #  tuples_city.append((city,provstateid))
            
            target = input_data['target'][i]
            targettype = input_data['targettype'][i]
            targettype_id = input_data['targettype_id'][i]
            try:
                if targetids[f'{target}'] != None:
                    targetid = targetids[f'{target}']
            except:
                targetid = len(targetids)+1
                targetids[f'{target}'] = targetid
                tuples_target.append((targetid,target,targettype_id,targettype))
            
            tuples_event.append((
                input_data['event_id'][i], 
                input_data['year'][i], 
                input_data['month'][i], 
                input_data['day'][i], 
                input_data['crit1'][i], 
                input_data['crit2'][i], 
                input_data['crit3'][i],
                input_data['success'][i],
                input_data['suicide'][i],
                input_data['attacktype_id'][i],
                input_data['attacktype'][i],
                input_data['gname'][i],
                input_data['individual'][i],
                input_data['nkill'][i],
                input_data['nwound'][i],
                input_data['property'][i],
                cityid,
                provstateid,
                countryid,
                regionid,
                targetid))

        try:  
            conn = mysql.connector.connect(host='localhost',
                                    port=13306, 
                                    database = 'odb',
                                    user='dbuser',
                                    password='secret')
            if conn.is_connected():
                    print('Connected to MySQL database')
            
            cursor = conn.cursor()
            #print(tuples_region)
            #print(tuples_country)
            #print(tuples_provstate)
            #print(tuples_city)
            
            for tuple in tqdm(tuples_region):
                cursor.execute(query_region,tuple)

            for tuple in tqdm(tuples_country):
                cursor.execute(query_country,tuple)
            
            for tuple in tqdm(tuples_provstate):
                cursor.execute(query_provstate,tuple)
            
            for tuple in tqdm(tuples_city):
                cursor.execute(query_city,tuple)

            for tuple in tqdm(tuples_target):
                cursor.execute(query_target,tuple)
            
            for tuple in tqdm(tuples_event):
                cursor.execute(query_event,tuple)
            
            conn.commit()
            
            cursor.execute("SELECT count(*) FROM country")
            res = cursor.fetchone()
        
            print('ODB is populated: {} new tuples are inserted'.format(len(tuples_country)+len(tuples_region)+len(tuples_provstate)+len(tuples_city)+len(tuples_target)+len(tuples_event)))
            print('                  {} total tuples are inserted'.format(res[0]))    
            
                
        except Error as e:
            print(e)
            
        finally:
            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()

def load_dw():
    # Connect to MySQL database
    odb_conn = None
    dw_conn = None
    adb_load_query = "INSERT INTO fact(fact_id, country, total_attacks) " \
                     "VALUES(%s,%s,%s)"
    odb_aggregate_query = "SELECT c.country, count(e.event_id) "\
                          " FROM country c AND event e "\
                          " GROUP BY c.country"    
                                     
    try:  
        odb_conn = mysql.connector.connect(host='localhost', 
                                  port=13306, 
                                  database = 'odb',
                                  user='dbuser',
                                  password='secret')
        
        dw_conn = mysql.connector.connect(host='localhost',
                                  port=23306, 
                                  database = 'dw',
                                  user='dbuser',
                                  password='secret')
        
        if odb_conn.is_connected():
                print('Connected to source ODB MySQL database')
                
        if dw_conn.is_connected():
                print('Connected to destination DW MySQL database')
        
        odb_cursor = odb_conn.cursor()
        dw_cursor = dw_conn.cursor()
        
        odb_cursor.execute(odb_aggregate_query)
        aggr_tuples = odb_cursor.fetchall()
    
        dw_cursor.executemany(adb_load_query,aggr_tuples)
        
        dw_conn.commit()
        
        dw_cursor.execute("SELECT count(*) FROM fact")
        res = dw_cursor.fetchone()
    
        print('DW is loaded: {} new tuples are inserted'.format(len(aggr_tuples)))
        print('               {} total tuples are inserted'.format(res[0]))    
        
            
    except Error as e:
        print(e)
        
    finally:
        if odb_conn is not None and odb_conn.is_connected():
            odb_cursor.close()
            odb_conn.close()
        if dw_conn is not None and dw_conn.is_connected():
            dw_cursor.close()
            dw_conn.close()    

if __name__ == '__main__':
    path = "/home/stiffi/Documents/master_2_sem/IKT453/project/stevensDW/data/terrorismdb_no_doubt.csv"
    #with open("columns.json", "rb") as f:
    #        columns = json.load(f)
    #columns = columns['columns']

    #print(columns)

    print(input_data.dtypes)
    #input_data = input_data[columns]
    #print(input_data)
    #print(len(input_data))

    #populate_odb(input_data)
    load_dw()
    