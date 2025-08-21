FROM apache/airflow:2.7.1

# Install additional Python packages as the non-root airflow user
USER airflow
RUN pip install --no-cache-dir --user requests psycopg2-binary
