FROM apache/airflow:2.5.2

USER root
RUN apt-get update -y
RUN apt-get install -y gcc

USER airflow
ADD requirements.txt .
RUN pip install -r requirements.txt