FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y git

RUN pip install dbt-core dbt-postgres

USER airflow
