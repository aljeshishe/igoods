#!/bin/sh
docker run -d --restart=always -p 8080:8080 \
    --name airflow \
    -v $(pwd)/requirements.txt:/requirements.txt  \
    -v $(pwd):/app -v $(pwd)/dags:/usr/local/airflow/dags \
    -v $(pwd)/airflow.cfg:/usr/local/airflow/airflow.cfg aljeshishe/airflow webserver