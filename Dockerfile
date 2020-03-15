FROM puckel/docker-airflow

WORKDIR /app
COPY . /app
COPY dag.py /usr/local/airflow/dags
#RUN conda env create -f requirements.yaml -p ./cenv
RUN pip install requests==2.23.0 pyspark==2.4.5

ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]