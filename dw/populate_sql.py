import json
import mysql.connector
import polars as pl
from tqdm import tqdm
from mysql.connector import Error


def populate_odb(input_data):
    # Connect to MySQL database
    conn = None

    query_region = "INSERT INTO region(regionID,region_txt) " \
            "VALUES(%s,%s)"
    #tuples_region = [(1,'Oslo Fylke', 1),(2,'Eastern US', 2)]    
    tuples_region = []


    query_country = "INSERT INTO country(countryID,country_txt, regionID) " \
            "VALUES(%s,%s,%s)"
    #tuples_country = [(1,'Norway'),(2,'USA')]
    tuples_country = []      
    

    query_provstate = "INSERT INTO provstate(provstate, countryID) " \
            "VALUES(%s,%s)"
    #tuples_provstate = [(1,'Oslo Kommune', 1),(2,'New York', 2)]
    tuples_provstate = []

    query_city = "INSERT INTO city(city, provstateID) " \
            "VALUES(%s,%s)"
    #tuples_city = [(1,'Oslo', 1),(2,'New York City', 2)]
    tuples_city = []

    for i in tqdm(range(len(input_data))):
        
        regionID = input_data['region'][i]
        region_txt = input_data['region_txt'][i]
        if (regionID,region_txt) not in tuples_region:
            tuples_region.append((regionID,region_txt))

        countryID = input_data['country'][i]
        country_txt = input_data['country_txt'][i]
        if (countryID,country_txt,regionID) not in tuples_country:
            tuples_country.append((countryID,country_txt,regionID))  
        
        provstate_txt = input_data['provstate'][i]
        if (provstate_txt,countryID) not in tuples_provstate:
            tuples_provstate.append((provstate_txt,countryID))

        city_txt = input_data['city'][i]
        # provstateID is the index of the provstate in provstate_txt
        provstateID = tuples_provstate.index((provstate_txt,countryID))+1
        if (city_txt,provstateID) not in tuples_city:
            tuples_city.append((city_txt,provstateID))

    try:  
        conn = mysql.connector.connect(host='localhost',
                                  port=13306, 
                                  database = 'odb',
                                  user='dbuser',
                                  password='secret')
        if conn.is_connected():
                print('Connected to MySQL database')
        
        cursor = conn.cursor()
        print(tuples_region)
        
        for tuple in tqdm(tuples_region):
            cursor.execute(query_region,tuple)

        for tuple in tqdm(tuples_country):
            cursor.execute(query_country,tuple)
        
        for tuple in tqdm(tuples_provstate):
            cursor.execute(query_provstate,tuple)
        
        for tuple in tqdm(tuples_city):
            cursor.execute(query_city,tuple)
        
            
        conn.commit()
        
        cursor.execute("SELECT count(*) FROM country")
        res = cursor.fetchone()
    
        print('ODB is populated: {} new tuples are inserted'.format(len(tuples_country)+len(tuples_region)+len(tuples_provstate)+len(tuples_city)))
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
    adb_load_query = "INSERT INTO fact(factid, eventID, iyear, imonth, iday, crit1, crit2, crit3, success, suicide, gname, individual, nkill, nwound, property, countryID, regionID, provstateID, cityID) " \
                     "VALUES(%s,%s,%s)"
    odb_aggregate_query = "SELECT location, product, sum(sale) "\
                          " FROM transaction "\
                          " GROUP BY location, product"    
                                     
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
    with open("columns.json", "rb") as f:
            columns = json.load(f)
    columns = columns['columns']

    print(columns)
    input_data = pl.read_csv(path, infer_schema_length=0)


    input_data = input_data.with_columns(
        pl.col("eventid").cast(pl.Int64),
        pl.col("iyear").cast(pl.Int64),
        pl.col("imonth").cast(pl.Int64),
        pl.col("iday").cast(pl.Int64),
        pl.col("country").cast(pl.Int64),
        pl.col("country_txt").cast(pl.String),
        pl.col("region").cast(pl.Int64),
        pl.col("region_txt").cast(pl.String),
        pl.col("provstate").cast(pl.String),
        pl.col("city").cast(pl.String),
        pl.col("crit1").cast(pl.Int64),
        pl.col("crit2").cast(pl.Int64),
        pl.col("crit3").cast(pl.Int64),
        pl.col("doubtterr").cast(pl.Int64),
        pl.col("success").cast(pl.Int64),
        pl.col("suicide").cast(pl.Int64),
        pl.col("attacktype1").cast(pl.Int64),
        pl.col("attacktype1_txt").cast(pl.String),
        pl.col("targtype1").cast(pl.Int64),
        pl.col("targtype1_txt").cast(pl.String),
        pl.col("target1").cast(pl.String),
        pl.col("gname").cast(pl.String),
        pl.col("individual").cast(pl.Int64),
        pl.col("nkill").cast(pl.Float64),
        pl.col("nwound").cast(pl.Float64),
        pl.col("property").cast(pl.Int64)
    )

    print(input_data.dtypes)
    #input_data = input_data[columns]
    #print(input_data)
    #print(len(input_data))

    populate_odb(input_data)
    #load_dw()
    