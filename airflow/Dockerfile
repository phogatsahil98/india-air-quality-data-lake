ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION=3.11

USER root

# Java 17 is required by the project's PySpark 3.5.x runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python
ENV SPARK_LOCAL_IP=127.0.0.1
ENV DBT_PROFILES_DIR=/home/airflow/.dbt
ENV DBT_BIN=/opt/dbt-venv/bin/dbt

# Keep dbt and Airflow dependency trees isolated.
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core==1.12.7" \
        "dbt-athena==1.11.2" \
    && chown -R airflow:root /opt/dbt-venv

COPY requirements.txt /tmp/requirements.txt

USER airflow

# Reinstall the exact Airflow version with official constraints before adding
# project packages. This prevents dependency resolution from upgrading Airflow.
RUN pip install --no-cache-dir \
      "apache-airflow[celery,fab,postgres,redis]==${AIRFLOW_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    && pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      -r /tmp/requirements.txt

WORKDIR /opt/airflow
