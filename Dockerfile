ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.5.1
FROM ${AIRFLOW_IMAGE_NAME}

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

RUN pip install --no-cache-dir python-dotenv

COPY ./src /opt/airflow/src

ENV PYTHONPATH=/opt/airflow/src:${PYTHONPATH}