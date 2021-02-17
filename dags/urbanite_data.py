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

#############################################################################
# Extract / Transform
#############################################################################

def fetchDataToLocal():
    """
    we use the python requests library to fetch the nyc in json format, then
    use the pandas library to easily convert from json to a csv saved in the
    local data directory
    """
    
    # fetching the request
    url = "https://api.citybik.es/v2/networks/bilbon-bizi"
    response = requests.get(url)

    # convert the response to a pandas dataframe, then save as csv to the data
    # folder in our project directory
    df = pd.DataFrame(json.loads(response.content))
    # df = df.set_index("date_of_interest")

    # for integrity reasons, let's attach the current date to the filename
    # df.to_csv("data/nyccovid_{}.csv".format(date.today().strftime("%Y%m%d")))
    df.to_json("urbanite_data_{}.json".format(date.today().strftime("%Y%m%d")))


#############################################################################
# Load
#############################################################################

def sqlLoad():
    """
    we make the connection to postgres using the psycopg2 library, create
    the schema to hold our covid data, and insert from the local csv file
    """
    
    # attempt the connection to postgres
    try:
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
    cursor = dbconnect.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS urbanite_data (
            date DATE,
            case_count INT,
            hospitalized_count INT,
            death_count INT,
            PRIMARY KEY (date)
        );
        
        TRUNCATE TABLE urbanite_data;
    """
    )
    dbconnect.commit()
    
	# Retrieve JSON => https://www.programiz.com/python-programming/json
    with open("urbanite_data_{}.json".format(date.today().strftime("%Y%m%d"))) as f:
	    data = json.load(f)
	    print(data)
		# print the keys and values
		#for key in data:
		#    value = data[key]
		#    print("The key and value are ({}) = ({})".format(key, value))

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
	        print("The free_bikes and id are ({}) = ({})".format(free_bikes, id))

		# TODO Escape
	    #cursor.execute("""
        #        INSERT INTO urbanite_data
        #        VALUES ('{}')
        #        """.format(data)
	    #)
    #dbconnect.commit()
	

default_args = { "owner": "airflow", "start_date": datetime.today() - timedelta(days=1)}

with DAG(
	"urbanite_data",
	default_args=default_args,
	schedule_interval = "0 1 * * *",
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

	