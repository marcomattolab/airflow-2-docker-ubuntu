import requests
import json
import pandas as pd
import psycopg2 as pg
from datetime import date
from configparser import ConfigParser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# reading the configuration file containing the postgres credentials
config = ConfigParser()
config.read("pg_creds.cfg")

# Enble Saving data on the PG DBMS
SAVE_DATA = True #TODO Create Variable
ETL_NAME = "api.citybik.es" #TODO Create Variable

#############################################################################
# Extract
#############################################################################

def fetchDataToLocal():
    """
    we use the python requests library to fetch the data in json format, then
    use the pandas library to easily convert from json to a txt saved in the
    local data directory
    """
    # See http://api.citybik.es/v2/
	# See https://api.citybik.es/v2/networks/velobike-moscow
    # fetching the request
    url = "https://api.citybik.es/v2/networks/bilbon-bizi"
    response = requests.get(url)

    # convert the response to a pandas dataframe, then save as file to the data
    # folder in our project directory
    df = pd.DataFrame(json.loads(response.content))
    # df = df.set_index("date_of_interest")

    # for integrity reasons, let's attach the current date to the filename
    df.to_json("urbanite_data_{}.json".format(date.today().strftime("%Y%m%d")))


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
                free_bikes_percentage numeric,
                PRIMARY KEY (etlname, etltimestamp, id)
            );
            
            TRUNCATE TABLE urbanite_data_perc;
        """
        )
        dbconnect.commit()
    curTimestamp = datetime.now()
    with open("urbanite_data_{}.json".format(date.today().strftime("%Y%m%d"))) as f:
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
	        # free_bikes_percentage = stationsArray[i]['free_bikes'] / stationsArray[i]['extra']['slots']
	        free_bikes_percentage = stationsArray[i]['free_bikes'] / stationsArray[i]['extra']['slots'] if (free_bikes < slots) else 1
			
	        print("id: {} free_bikes: {}".format(id, free_bikes))
			
	        if SAVE_DATA:
	            # print("INSERT INTO urbanite_data_perc VALUES ('{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}' , '{}', '{}')".format(ETL_NAME, curTimestamp, id, timestamp, name, latitude, longitude, slots, empty_slots, free_bikes, number, free_bikes_percentage))
	            cursor.execute("""
	            INSERT INTO urbanite_data_perc
	            VALUES ('{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
	            """.format(ETL_NAME, curTimestamp, id, timestamp, name, latitude, longitude, slots, empty_slots, free_bikes, number, free_bikes_percentage)				
	            )
	    if SAVE_DATA :
	        dbconnect.commit()
	

default_args = { 
    "owner": "airflow", 
    "start_date": datetime.today() - timedelta(days=1)
}

with DAG(
	"urbanite_data",
	default_args=default_args,
	schedule_interval = "*/15 * * * *",
	) as dag:

	fetchDataToLocal = PythonOperator(
			task_id="fetch_data_to_local",
			python_callable=fetchDataToLocal
		)

	sqlLoad = PythonOperator(
			task_id="sql_load",
			python_callable=sqlLoad
		)

	fetchDataToLocal >> sqlLoad
