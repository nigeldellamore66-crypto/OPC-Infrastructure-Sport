FROM apache/airflow:2.10.3-python3.8

USER root

# Java 17 pour Spark client
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        wget \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Spark client
RUN wget -qO- https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz \
        | tar xz -C /opt \
    && mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark \
    && chown -R airflow:root /opt/spark

# JARs additionnels (mêmes versions que docker/spark/Dockerfile pour compatibilité)
RUN cd /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.11.0