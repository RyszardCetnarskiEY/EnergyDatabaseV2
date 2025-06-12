# Dockerfile
FROM apache/airflow:2.9.3

#USER root
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# revert back for Airflow runtime
#USER airflow
