import airflow
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['danilotrombino@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}
dag = DAG(
    'docker_operator_example',          # DAG ID
    default_args=default_args,
    schedule_interval='1 * * * *', # Run every hour
    catchup=False
)

with dag as dag:
    t1 = DockerOperator(
                task_id='docker_sleep_10s',
                image='alpine:3.8',
                api_version='auto',
                auto_remove=True,
                command="/bin/sleep 10",
                do_xcom_push = False
    )
    t2 = DockerOperator(
                task_id='docker_sleep_15s',
                image='alpine:3.8',
                api_version='auto',
                auto_remove=True,
                command="/bin/sleep 15",
                do_xcom_push = False
    )
    t3 = DockerOperator(
                task_id='docker_sleep_20s',
                image='alpine:3.8',
                api_version='auto',
                auto_remove=True,
                command="/bin/sleep 20",
                do_xcom_push = False
    )
    t4 = DockerOperator(
                task_id='docker_sleep_5s',
                image='alpine:3.8',
                api_version='auto',
                auto_remove=True,
                command="/bin/sleep 5",
                do_xcom_push = False
    )

    t1 >> t2
    t1 >> t3
    t2 >> t4
    t3 >> t4