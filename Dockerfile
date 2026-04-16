# PySpark batch job: Python + JVM for Spark (PostgreSQL JDBC driver pulled at runtime via Spark packages).
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY docker-entrypoint.sh /docker-entrypoint.sh
# Windows checkouts may use CRLF; Linux then fails with "no such file or directory" on exec.
RUN sed -i 's/\r$//' /docker-entrypoint.sh && chmod +x /docker-entrypoint.sh

ENV SPARK_DRIVER_MEMORY=512m \
    SPARK_SHUFFLE_PARTITIONS=4

ENTRYPOINT ["/bin/sh", "/docker-entrypoint.sh"]

# Downstream: set WATER_ETL_USE_POSTGRES, DB credentials, JDBC URL (see .env.example).
CMD ["python", "main.py"]
