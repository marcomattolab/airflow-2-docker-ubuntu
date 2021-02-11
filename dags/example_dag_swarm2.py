import airflow
from airflow import DAG
from airflow.providers.docker.operators.docker_swarm import DockerSwarmOperator

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['danilotrombino@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}
dag = DAG(
    'docker_swarm_example2',          # DAG ID
    default_args=default_args,
    schedule_interval='1 * * * *', # Run every hour
    catchup=False
)

with dag as dag:
    t1 = DockerSwarmOperator(
                task_id='swarm_sleep_30s',
                image='alpine:3.8',
                api_version='auto',
                command="/bin/sleep 10",
                auto_remove=True,
                do_xcom_push = False,
                enable_logging = False,
                tty=True
    )
    t2 = DockerSwarmOperator(
                task_id='swarm_sleep_15s',
                image='alpine:3.8',
                api_version='auto',
                command="/bin/sleep 15",
                auto_remove=True,
                do_xcom_push = False,
                enable_logging = False,
                tty=True
    )
    t3 = DockerSwarmOperator(
                task_id='swarm_sleep_20s',
                image='alpine:3.8',
                api_version='auto',
                command="/bin/sleep 10",
                auto_remove=True,
                do_xcom_push = False,
                enable_logging = False,
                tty=True
    )
    t4 = DockerSwarmOperator(
                task_id='swarm_sleep_5s',
                image='alpine:3.8',
                api_version='auto',
                command="/bin/sleep 5",
                auto_remove=True,
                do_xcom_push = False,
                enable_logging = False,
                tty=True
    )

    t1 >> t2
    t1 >> t3
    t2 >> t4
    t3 >> t4