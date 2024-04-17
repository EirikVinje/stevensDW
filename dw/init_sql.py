import json
import datetime
import pycountry_convert
import mysql.connector
import polars as pl
import numpy as np
from tqdm import tqdm
from mysql.connector import Error
import os

class TerroristSQLDatabase:
    def __init__(self, path):

        self.DB_USER = 'dbuser'
        self.DB_PASSWORD = 'secret'
        self.DB_HOST = 'localhost' 
        self.DB_NAME = 'dw'

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
        
        self.raw = self.raw.with_columns(
            pl.col("nkill").cast(pl.Int64),
            pl.col("nwound").cast(pl.Int64),
        )

        try:
            conn = mysql.connector.connect(host=self.DB_HOST, 
                                    port=13306, 
                                    user=self.DB_USER,
                                    password=self.DB_PASSWORD)
        
            cursor = conn.cursor()
            cursor.execute(f"USE odb")
            print(f"Database odb is connected")

            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()       

        except:            
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
                        print('Connected to odb, creating tables...')
            
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
            self.populate_db(self.raw)
        try:
            conn = mysql.connector.connect(host='localhost', 
                                    port=23306, 
                                    user='dbuser',
                                    password='secret')
        
            cursor = conn.cursor()
            cursor.execute(f"USE dw")
            print(f"Database {self.DB_NAME} is connected")

            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()
        except:
            conn = None
            create_db = " CREATE DATABASE dw"
            use_db = "use dw"
            create_table = "CREATE TABLE fact (fact_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                            "year INT, month INT, day INT, crit1 INT, crit2 INT, crit3 INT, success INT, suicide INT, attacktype VARCHAR(100), gname VARCHAR(400), " \
                            "individual INT, nkill INT, nwound INT, property INT, country VARCHAR(100), region VARCHAR(100), provstate VARCHAR(100), city VARCHAR(100), target VARCHAR(400), targettype VARCHAR(100)) "
            try:  
                conn = mysql.connector.connect(host=self.DB_HOST,
                                        port=23306, 
                                        user=self.DB_USER,
                                        password=self.DB_PASSWORD)
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
                self.load_dw()

    def read_data(self, table_name : str):

        conn = None
        try:
            conn = mysql.connector.connect(host=self.DB_HOST, 
                                    port=23306, 
                                    database = self.DB_NAME,
                                    user=self.DB_USER,
                                    password=self.DB_PASSWORD)
            if conn.is_connected():
                    print('Connected to database, selecting', table_name)
        
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name};")
            for res in cursor.fetchall():
                print(res)
        
        except Error as e:
            print(e)
            
        finally:
            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()
    
    def populate_db(self, input_data : pl.DataFrame):
        conn = None

        query_region = "INSERT INTO region(region_id,region) " \
                "VALUES(%s,%s)" 
        tuples_region = []


        query_country = "INSERT INTO country(country_id,country, region_id) " \
                "VALUES(%s,%s,%s)"
        tuples_country = []      
        

        query_provstate = "INSERT INTO provstate(provstate_id,provstate, country_id) " \
                "VALUES(%s,%s,%s)"
        tuples_provstate = []

        query_city = "INSERT INTO city(city_id,city, provstate_id) " \
                "VALUES(%s,%s,%s)"
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

            city = input_data['city'][i]
            try:
                if cityids[f'{provstate},{city}'] != None:
                    cityid = cityids[f'{provstate},{city}']
            except:
                cityid = len(cityids)+1
                cityids[f'{provstate},{city}'] = cityid
                tuples_city.append((cityid,city,provstateid))
            
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
            conn = mysql.connector.connect(host=self.DB_HOST, 
                                    port=13306, 
                                    database = 'odb',
                                    user=self.DB_USER,
                                    password=self.DB_PASSWORD)
            if conn.is_connected():
                    print('Connected to database, populating tables...')
            
            cursor = conn.cursor()
            
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
            
            cursor.execute("SELECT count(*) FROM event")
            res = cursor.fetchone()
        
            print('DB is populated: {} new tuples are inserted'.format(len(tuples_country)+len(tuples_region)+len(tuples_provstate)+len(tuples_city)+len(tuples_target)+len(tuples_event)))
            print('                  {} total tuples are inserted'.format(res[0]))    
            
                
        except Error as e:
            print(e)
            
        finally:
            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()
    
    def load_dw(self):
        odb_conn = None
        dw_conn = None
        adb_load_query = "INSERT INTO fact(year, month, day, crit1, crit2, crit3, success, suicide, attacktype, gname, " \
                            "individual, nkill, nwound, property, country, region, provstate, city, target, targettype) " \
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        odb_aggregate_query = "SELECT e.year, e.month, e.day, e.crit1, e.crit2, e.crit3, e.success, e.suicide, e.attacktype, e.gname, "\
                            "e.individual, e.nkill, e.nwound, e.property, country.country, region.region, provstate.provstate, city.city, target.target, target.targettype " \
                            "FROM event e, country, region, provstate, city, target "\
                            "WHERE e.country_id=country.country_id AND e.region_id=region.region_id AND e.provstate_id=provstate.provstate_id AND e.city_id=city.city_id AND e.target_id=target.target_id"    
                                        
        try:  
            odb_conn = mysql.connector.connect(host=self.DB_HOST, 
                                    port=13306, 
                                    database = 'odb',
                                    user=self.DB_USER,
                                    password=self.DB_PASSWORD)
            
            dw_conn = mysql.connector.connect(host=self.DB_HOST, 
                                    port=23306, 
                                    database = self.DB_NAME,
                                    user=self.DB_USER,
                                    password=self.DB_PASSWORD)
            
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
    
    def get_num_events_all_countries(self):
        conn = None
        try:
            conn = mysql.connector.connect(host=self.DB_HOST, 
                                    port=23306, 
                                    database = self.DB_NAME,
                                    user=self.DB_USER,
                                    password=self.DB_PASSWORD)
            if conn.is_connected():
                    print('Connected to database, getting number of events for all countries')
        
            cursor = conn.cursor()
            cursor.execute("SELECT country, COUNT(*) count FROM fact GROUP BY country ORDER BY count DESC;")
            res = cursor.fetchall()
            #for r in res:
            #    print(r)
            
            data = []

            for i in res:
                #print(i[0])
                try:
                    country = pycountry_convert.country_name_to_country_alpha3(i[0])
                    if country:
                        data.append([country, i[1], i[0]])
                except:
                    continue

            df = pl.DataFrame(data, schema=["iso_alpha", "count", "country"])
            
        
        except Error as e:
            print(e)
            
        finally:
            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()
        return df
    
    def get_events_by_country(self, country_name : str):
        conn = None
        try:
            conn = mysql.connector.connect(host=self.DB_HOST, 
                                    port=23306, 
                                    database = self.DB_NAME,
                                    user=self.DB_USER,
                                    password=self.DB_PASSWORD)
            if conn.is_connected():
                    print('Connected to database, getting events by country')
        
            cursor = conn.cursor()
            cursor.execute(f"SELECT year, CAST(SUM(nkill) AS UNSIGNED), COUNT(fact_id) FROM fact WHERE country='{country_name}' GROUP BY year;")

            res = cursor.fetchall()
            #for r in res:
            #    print(r)
            
            data = []

            for i in res:
                data.append(i)
            df = pl.DataFrame(data, schema=["year", "nkill", "num_events"])          
            
        except Error as e:
            print(e)
            
        finally:
            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()
        return df

    def get_events_with_criteria(self, country=None, start_year=None, end_year=None, attack_type=None, target_type=None, success=None):
        # return dataframe with the events that matches
        conn = None
        try:
            conn = mysql.connector.connect(host=self.DB_HOST, 
                                    port=23306, 
                                    database = self.DB_NAME,
                                    user=self.DB_USER,
                                    password=self.DB_PASSWORD)
            if conn.is_connected():
                    print(f'Connected to database, getting events in {country} from {start_year} to {end_year}')
            
            cursor = conn.cursor()

            sql_query = "SELECT year, month, day, region, country, provstate, city, target, targettype, success, suicide, attacktype, gname, individual, nkill, nwound, property FROM fact WHERE 1=1"

            if country is not None:
                sql_query += f" AND country='{country}'"
            if attack_type is not None:
                sql_query += f" AND attacktype='{attack_type}'"
            if target_type is not None:
                sql_query += f" AND targettype='{target_type}'"
            if success is not None:
                sql_query += f" AND success={success}"
            if start_year is not None:
                sql_query += f" AND year >= {start_year}"
            if end_year is not None:
                sql_query += f" AND year <= {end_year}"
            
            cursor.execute(sql_query)

            res = cursor.fetchall()
            #for r in res:
            #    print(r)
            
            data = []

            for i in res:
                data.append(i)

            df = pl.DataFrame(data, schema=["year", "month", "day","region","country","provstate","city","target","targettype", "success","suicide","attacktype","gname","individual","nkill","nwound","property"])
            
        
        except Error as e:
            print(e)
            
        finally:
            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()
        return df

if __name__ == '__main__':
    
    cwd = os.getcwd()

    path = f"{cwd}/data/terrorismdb_no_doubt.csv"
    db = TerroristSQLDatabase(path)
    #db.populate_db(db.raw)
    #db.read_data('country')
    #db.get_num_events_all_countries()
    #df = db.get_events_by_country(country_name='United States')
    #db.get_events_by_region(region_name="Central Asia")
    #db.get_event_in_country_from_start_to_end(country_name="Norway",start_year=2011, end_year=2012)
    #df =db.get_events_with_criteria(country='Norway', start_year=1980, end_year=2012, attacktype='Bombing/Explosion', targettype='Government (General)', success=1)
    #df = db.get_events_with_criteria(country='United States')
    #print(df)
    