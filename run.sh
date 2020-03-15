#!/bin/sh
docker run -d --restart=always -p 8080:8080 -v $(pwd)/requirements.txt:/requirements.txt  \
    -v $(pwd):/app -v $(pwd)/dags:/usr/local/airflow/dags \
    -v $(pwd)/airflow.cfg:/usr/local/airflow/airflow.cfg puckel/docker-airflow webserver