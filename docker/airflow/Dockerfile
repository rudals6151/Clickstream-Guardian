FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.5.0 \
    apache-airflow-providers-postgres==5.10.0 \
    boto3==1.34.0 \
    psycopg2-binary==2.9.9

# Copy entrypoint script
COPY --chown=airflow:root entrypoint.sh /entrypoint.sh
USER root
RUN chmod +x /entrypoint.sh
USER airflow

ENTRYPOINT ["/entrypoint.sh"]
