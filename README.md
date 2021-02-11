# Airflow2Docker

Airflow2Docker is a repository usefull to install airflow 2.0 with docker. (Without SWARM)
See also: https://github.com/trowdan/airflow2-docker


## Steps:
1. Clone project: "git clone https://gitlab.com/marco.martorana/airflow2docker.git"
2. Set env variables on .env files
3. Build docker image: "sudo docker build -t airflow2-docker:1.0.0 ."
4. Up docker service:  "sudo docker-compose up"