#
# Demo Urbanite License
#
# See: https://towardsdatascience.com/schedule-web-scrapers-with-apache-airflow-3c3a99a39974
# See: https://blog.insightdatascience.com/airflow-101-start-automating-your-batch-workflows-with-ease-8e7d35387f94
# See: https://airflow.apache.org/docs/apache-airflow/2.0.1/_modules/airflow/example_dags/tutorial_taskflow_api_etl.html
"""Demo Urbanite """
import time
import json
from datetime import timedelta
from pprint import pprint

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['demo_urbanite@demo_urbanite.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('demo_urbanite', default_args=default_args, tags=['demo_urbanite'], start_date=days_ago(2))

dag.doc_md = __doc__

# [START howto_operator_python]
def print_context(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    task_id='print_the_context',
    python_callable=print_context,
    dag=dag,
)
# [END howto_operator_python]

# [START howto_operator_python_kwargs]
#def my_sleeping_function(random_base):
#    """This is a function that will run within the DAG execution"""
#    time.sleep(random_base)
#
# Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
#for i in range(5):
#    task = PythonOperator(
#        task_id='sleep_for_' + str(i),
#        python_callable=my_sleeping_function,
#        op_kwargs={'random_base': float(i) / 10},
#        dag=dag,
#    )

#    run_this >> task
# [END howto_operator_python_kwargs]

# [START howto_operator_http_task_get_op]
task_get_op = SimpleHttpOperator(
    task_id='get_op',
	http_conn_id='http_default',
    method='GET',
    endpoint='get',
    data={"param1": "value1", "param2": "value2"},
    headers={},
	log_response=True,
	#xcom_push=True,
	#headers={"Content-Type": "application/json"},
    dag=dag,
)
# [END howto_operator_http_task_get_op]
# [START howto_operator_http_task_get_op_response_filter]
#task_get_op_response_filter = SimpleHttpOperator(
#    task_id='get_op_response_filter',
#	http_conn_id='http_default',
#    method='GET',
#    endpoint='get',
#    response_filter=lambda response: response.json()['nested']['property'],
#	 log_response=True,
#    dag=dag,
#)


#task_get_op >> task_get_op_response_filter >> run_this
task_get_op >> run_this