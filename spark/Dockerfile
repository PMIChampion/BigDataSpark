FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y curl python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    python3 -m pip install --no-cache-dir psycopg2-binary==2.9.9 && \
    curl -fsSL -o /opt/spark/jars/postgresql-42.7.3.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    curl -fsSL -o /opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar \
      https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar

WORKDIR /opt/app
