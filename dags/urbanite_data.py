import requests
import json
import pickle
from decimal import Decimal
import pandas as pd
import psycopg2 as pg
from datetime import date
from configparser import ConfigParser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# reading the configuration file containing the postgres credentials
config = ConfigParser()
config.read("pg_creds.cfg")

# Variables
CURRENT_TIMESTAMP = datetime.now()
ETL_NAME = Variable.get ("ETL_NAME_ES")
SAVE_DATA = Variable.get ("ETL_SAVE_DATA_ES")
ETL_URL = Variable.get ("ETL_URL_ES")

#See http://api.citybik.es/v2/
#ETL_NAME = "api.citybik.es"
#SAVE_DATA = True
#ETL_URL = "https://api.citybik.es/v2/networks/bilbon-bizi"


#############################################################################
# CustomJsonEncoder
#############################################################################

class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(CustomJsonEncoder, self).default(obj)


#############################################################################
# Extract
#############################################################################

def fetchDataToLocal():
    """
    we use the python requests library to fetch the data in json format, then
    use the pandas library to easily convert from json to a txt saved in the
    local data directory
    """

    # fetching the request
    response = requests.get(ETL_URL)

    # convert the response to a pandas dataframe, then save as file to the data
    # folder in our project directory
    df = pd.DataFrame(json.loads(response.content))
    # df = df.set_index("date_of_interest")

    # for integrity reasons, let's attach the current date to the filename
    df.to_json("/opt/airflow/logs/urbanite_data_{}.json".format(date.today().strftime("%Y%m%d")))


#############################################################################
# Transform / Load 
#############################################################################

def sqlLoad():
    """
    we make the connection to postgres using the psycopg2 library, create
    the schema to hold our covid data, and insert from the local txt file
    """

    # attempt the connection to postgres
    try:
        #TODO USE VARIABLES
        #dbconnect = pg.connect(
        #    database=config.get("postgres", "DATABASE"),
        #    user=config.get("postgres", "USERNAME"),
        #    password=config.get("postgres", "PASSWORD"),
        #    host=config.get("postgres", "HOST")
        #)
        dbconnect = pg.connect(
            database="airflow",
            user="airflow",
            password="airflow",
            host="postgres"
        )
    except Exception as error:
        print(error)
    
    # create the table if it does not already exist
    if SAVE_DATA:
        cursor = dbconnect.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS urbanite_data_perc (
            	etlname VARCHAR(1000),
				etltimestamp TIMESTAMP without time zone,
				id VARCHAR(1000),
				timestamp VARCHAR(1000),
				name VARCHAR(1000),
                latitude VARCHAR(1000),
                longitude VARCHAR(1000),
				slots INT,
				empty_slots INT,
                free_bikes INT,
				number INT,
                free_bikes_perc numeric,
                PRIMARY KEY (etlname, etltimestamp, id)
            );
            
        """
        )
        dbconnect.commit()

    #curTimestamp = datetime.now()
    with open("/opt/airflow/logs/urbanite_data_{}.json".format(date.today().strftime("%Y%m%d"))) as f:
	    data = json.load(f)
	    print(data)

	    stationsArray = data["network"]["stations"]
	    for i in range(len(stationsArray)):
	        id = stationsArray[i]['id']
	        name = stationsArray[i]['name']
	        timestamp = stationsArray[i]['timestamp']
	        latitude = stationsArray[i]['latitude']
	        longitude = stationsArray[i]['longitude']
	        free_bikes = stationsArray[i]['free_bikes']
	        empty_slots = stationsArray[i]['empty_slots']
	        bike_uids = stationsArray[i]['extra']['bike_uids']
	        uid = stationsArray[i]['extra']['uid']
	        number = stationsArray[i]['extra']['number']
	        slots = stationsArray[i]['extra']['slots']
	        
			# This is a transformation in Percentage
	        free_bikes_perc = stationsArray[i]['free_bikes'] / stationsArray[i]['extra']['slots'] if (free_bikes < slots) else 1
	        print("id: {} free_bikes_perc: {}".format(id, free_bikes_perc))
			
	        if SAVE_DATA:
	            # print("INSERT INTO urbanite_data_perc VALUES ('{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}' , '{}', '{}')".format(ETL_NAME, CURRENT_TIMESTAMP, id, timestamp, name, latitude, longitude, slots, empty_slots, free_bikes, number, free_bikes_perc))
	            cursor.execute("""
	            INSERT INTO urbanite_data_perc
	            VALUES ('{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
	            """.format(ETL_NAME, CURRENT_TIMESTAMP, id, timestamp, name, latitude, longitude, slots, empty_slots, free_bikes, number, free_bikes_perc)				
	            )
	    if SAVE_DATA :
	        dbconnect.commit()
	
#############################################################################
# Retrieve Trasformed Data
#############################################################################

def retrieveTrasformedData():
    # attempt the connection to postgres
    try:
        dbconnect = pg.connect(
            database="airflow",
            user="airflow",
            password="airflow",
            host="postgres"
        )
    except Exception as error:
        print(error)
	

    if SAVE_DATA:
        cursor = dbconnect.cursor()
        
        lowerDistribution = []
        mediumDistribution = []
        hightDistribution = []
        jsonTotalDistribution= {"lowerDistribution": lowerDistribution,
								"mediumDistribution": mediumDistribution, 
								"hightDistribution": hightDistribution}	

        cursor.execute("""
            SELECT id, timestamp, name, latitude, longitude, slots, empty_slots, free_bikes, number, free_bikes_perc 
			from urbanite_data_perc 
			WHERE etlname = '{}' 
			and etltimestamp = (select max(etltimestamp) from urbanite_data_perc where etlname = '{}' ) 
			and free_bikes_perc >= 0 
			and free_bikes_perc < 0.3;
			""".format(ETL_NAME, ETL_NAME))
        sources = cursor.fetchall()
        for source in sources:
            jsonTotalDistribution['lowerDistribution'].append({"id": source[0], "timestamp": source[1], "name": source[2], "latitude": source[3], "longitude": source[4], "slots": source[5] , "empty_slots": source[6], "free_bikes": source[7], "number": source[8], "free_bikes_perc": source[9]});
            print(" Lower. RetrieveTrasformedData ==>  '{}', '{}', '{}', '{}', '{}','{}', '{}', '{}')".format(source[0],source[1],source[2],source[3],source[4],source[5],source[6],source[7]))
		
        cursor.execute("""
            SELECT id, timestamp, name, latitude, longitude, slots, empty_slots, free_bikes, number, free_bikes_perc 
			from urbanite_data_perc 
			WHERE etlname = '{}' 
			and etltimestamp = (select max(etltimestamp) from urbanite_data_perc where etlname = '{}' ) 
			and free_bikes_perc > 0.3 
			and free_bikes_perc <= 0.6;
			""".format(ETL_NAME, ETL_NAME))
        sources = cursor.fetchall()
        for source in sources:
            jsonTotalDistribution['mediumDistribution'].append({"id": source[0], "timestamp": source[1], "name": source[2], "latitude": source[3], "longitude": source[4], "slots": source[5] , "empty_slots": source[6], "free_bikes": source[7], "number": source[8], "free_bikes_perc": source[9]});
            print(" Medium. RetrieveTrasformedData ==>  '{}', '{}', '{}', '{}', '{}','{}', '{}', '{}')".format(source[0],source[1],source[2],source[3],source[4],source[5],source[6],source[7]))
		
        cursor.execute("""
            SELECT id, timestamp, name, latitude, longitude, slots, empty_slots, free_bikes, number, free_bikes_perc 
			from urbanite_data_perc 
			WHERE etlname = '{}' 
			and etltimestamp = (select max(etltimestamp) from urbanite_data_perc where etlname = '{}' ) 
			and free_bikes_perc > 0.6 
			and free_bikes_perc <= 1;
			""".format(ETL_NAME, ETL_NAME))
        sources = cursor.fetchall()
        for source in sources:
            jsonTotalDistribution['hightDistribution'].append({"id": source[0], "timestamp": source[1], "name": source[2], "latitude": source[3], "longitude": source[4], "slots": source[5] , "empty_slots": source[6], "free_bikes": source[7], "number": source[8], "free_bikes_perc": source[9]});
            print(" Hight. RetrieveTrasformedData ==>  '{}', '{}', '{}', '{}', '{}','{}', '{}', '{}')".format(source[0],source[1],source[2],source[3],source[4],source[5],source[6],source[7]))
        
        with open("/opt/airflow/logs/urbanite_data_generated_{}.json".format(date.today().strftime("%Y%m%d")), 'w') as file:
            file.write(json.dumps(jsonTotalDistribution, cls=CustomJsonEncoder))
        
        print("URBANITE:= Algorthm has been succesfully completed and file generated!!!")
        
		
default_args = { 
    "owner": "airflow", 
    "start_date": datetime.today() - timedelta(days=1)
}

with DAG(
	"urbanite_data",
	default_args=default_args,
	schedule_interval = "*/30 * * * *",
	) as dag:

	fetchDataToLocal = PythonOperator(
			task_id="fetch_data_to_local",
			python_callable=fetchDataToLocal
		)

	sqlLoad = PythonOperator(
			task_id="sql_load",
			python_callable=sqlLoad
		)

	retrieveTrasformedData = PythonOperator(
			task_id="retrieve_trasformed_data",
			python_callable=retrieveTrasformedData
		)
	fetchDataToLocal >> sqlLoad >> retrieveTrasformedData
