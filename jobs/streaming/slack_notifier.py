"""
Job Spark Structured Streaming.

Consomme le topic Redpanda `cdc.ops.activities` et envoie une notification Slack
pour chaque nouvelle activité 'live'
"""
import notifier  
import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, TimestampType
)

# Import du module local (on l'ajoute au PYTHONPATH dans le DAG)
sys.path.insert(0, "/opt/spark/jobs/streaming")

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("slack-notifier")


# Configuration

KAFKA_BOOTSTRAP = "redpanda:9092"
TOPIC = "cdc.ops.activities"
CHECKPOINT_LOCATION = "s3a://lakehouse/_checkpoints/slack_notifier"


# Schéma du corps Debezium pour traitement par spark 
ACTIVITY_SCHEMA = StructType([
    StructField("activity_id", LongType(), nullable=True),
    StructField("employee_id", IntegerType(), nullable=True),
    StructField("sport_type", StringType(), nullable=True),
    StructField("start_at", StringType(), nullable=True), 
    StructField("end_at", StringType(), nullable=True),
    StructField("distance_m", IntegerType(), nullable=True),
    StructField("duration_s", IntegerType(), nullable=True),
    StructField("comment", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
    StructField("__op", StringType(), nullable=True), # Champ créé par Debezium: déclaration du type d'opération ( création, suppression...)
    StructField("__source_ts_ms", LongType(), nullable=True), # Champ créé par Debezium: timestamp de l'évenement
])


# Logique de traitement par batch
def process_batch(batch_df, batch_id: int):
    """
    Appelée pour chaque micro-batch.
    Pour chaque ligne du batch, on fait un lookup Postgres et un POST Slack.

    """
    # Si batch vide, on ne fait rien
    if batch_df.rdd.isEmpty():
        return

    # Collecte les lignes d'évenements sous forme de liste python
    rows = batch_df.collect()
    log.info("Batch %d: %d activité(s) live à notifier", batch_id, len(rows))

    for r in rows:
        try:
            # Récupère le nom de l'employé 
            full_name = notifier.fetch_employee_name(r["employee_id"])
            # Formate le message au format slack
            message = notifier.format_slack_message(
                full_name=full_name,
                sport_type=r["sport_type"],
                distance_m=r["distance_m"],
                duration_s=r["duration_s"],
                comment=r["comment"],
            )
            notifier.send_to_slack(message) # Envoie le message à Slack
        except Exception as e:
            log.exception("Erreur traitement activité %s: %s", r["activity_id"], e)


# Point d'entrée Spark
def main():
    log.info("Démarrage du job slack-notifier")

    # Construction de la session spark
    spark = (
        SparkSession.builder
        .appName("slack-notifier")
        # Configuration S3 pour les checkpoints sur MinIO
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Lecture du topic Redpanda
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")    # ignore l'historique au démarrage
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parsing du corps JSON et application des filtres
    activities = (
        raw_stream
        .select(
            F.from_json(F.col("value").cast("string"), ACTIVITY_SCHEMA).alias("data") # convertit le binaire en chaine, puis parse selon le schéma défini
        )
        .select("data.*")
        .filter(F.col("__op") == "c")           # uniquement les créations
        .filter(F.col("source") == "live")      # uniquement les activités live
        .filter(F.col("employee_id").isNotNull())
    )

    # 3. Écriture vers Slack via foreachBatch
    query = (
        activities.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="2 seconds")   # micro-batch toutes les 2s
        .start()
    )

    log.info("Job en cours. Topic: %s | Checkpoint: %s", TOPIC, CHECKPOINT_LOCATION)
    query.awaitTermination()


if __name__ == "__main__":
    main()
