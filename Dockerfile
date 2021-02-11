FROM apache/airflow:2.0.0-python3.7

USER root

# INSTALL TOOLS
RUN apt-get update \
    && apt-get -y install unzip \
    && apt-get -y install libaio-dev \
    && apt-get install postgresql-client  

# INSTALL DEPENDENCIES
RUN pip install --no-cache-dir pandas boto3 xlrd==1.2.0

RUN mkdir extra

RUN groupadd --gid 999 docker \
    && usermod -aG docker airflow

USER airflow

# COPY SQL SCRIPT
COPY scripts/airflow/check_init.sql ./extra/check_init.sql
COPY scripts/airflow/set_init.sql ./extra/set_init.sql

# ENTRYPOINT SCRIPT
COPY scripts/airflow/init.sh ./init.sh

ENTRYPOINT ["./init.sh"]