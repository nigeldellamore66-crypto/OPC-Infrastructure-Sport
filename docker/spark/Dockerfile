FROM apache/spark:3.5.3-python3

USER root

# Téléchargement des jars
RUN set -eux; \
    cd /opt/spark/jars; \
    # Delta Lake 3.2.x (compatible Spark 3.5)
    curl -fLO https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar; \
    curl -fLO https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar; \
    # Kafka pour Spark Structured Streaming
    curl -fLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar; \
    curl -fLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar; \
    curl -fLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar; \
    curl -fLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; \
    # S3 (MinIO)
    curl -fLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar; \
    curl -fLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar; \
    # Postgres JDBC pour le serving
    curl -fLO https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# Dépendances Python utiles dans les jobs
RUN pip install --no-cache-dir \
        delta-spark==3.2.1 \
        requests==2.32.3 \
        psycopg2-binary==2.9.10

USER spark
