# dependencies
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
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)

    # convert the response to a pandas dataframe, then save as csv to the data
    # folder in our project directory
    df = pd.DataFrame(json.loads(response.content))
    df = df.set_index("date_of_interest")

    # for integrity reasons, let's attach the current date to the filename
    # df.to_csv("data/nyccovid_{}.csv".format(date.today().strftime("%Y%m%d")))
    df.to_csv("nyccovid_{}.csv".format(date.today().strftime("%Y%m%d")))


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
        CREATE TABLE IF NOT EXISTS covid_data (
            date DATE,
            case_count INT,
            hospitalized_count INT,
            death_count INT,
            PRIMARY KEY (date)
        );
        
        TRUNCATE TABLE covid_data;
    """
    )
    dbconnect.commit()
    
    # insert each csv row as a record in our database
    #with open("data/nyccovid_{}.csv".format(date.today().strftime("%Y%m%d"))) as f:
    with open("nyccovid_{}.csv".format(date.today().strftime("%Y%m%d"))) as f:
        next(f) # skip the first row (header)
        for row in f:
            cursor.execute("""
                INSERT INTO covid_data
                VALUES ('{}', '{}', '{}', '{}')
            """.format(
            row.split(",")[0],
            row.split(",")[1],
            row.split(",")[2],
            row.split(",")[3])
            )
    dbconnect.commit()
	


default_args = { "owner": "airflow", "start_date": datetime.today() - timedelta(days=1)}

with DAG(
	"covid_nyc_data",
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




	