"""
Copie brute Postgres → Delta Lake (couche Bronze du médaillon).

Lit hr.employees et ops.activities depuis Postgres,
les écrit en Delta sur MinIO, partitionnées par date d'extraction.
"""

import logging
import os
import sys
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("bronze")

# Connection à la base postgres
JDBC_URL = "jdbc:postgresql://postgres:5432/sportdata"
JDBC_PROPERTIES = {
    "user":     os.getenv("POSTGRES_USER", "sportadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
    "driver":   "org.postgresql.Driver",
}

BRONZE_BASE = "s3a://lakehouse/bronze"


def ingest_table(spark, source_table, target_path):
    """Lit une table Postgres et l'écrit en Delta partitionnée par extract_date."""
    log.info("Ingestion de %s vers %s", source_table, target_path)

    # Lecture spark de la table sélectionnée
    df = (
        spark.read
        .jdbc(url=JDBC_URL, table=source_table, properties=JDBC_PROPERTIES)
        .withColumn("extract_date", F.lit(date.today()))
        .withColumn("extract_ts", F.current_timestamp())
    )
    # Ecriture au format delta dans la cible MiniO
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"extract_date = '{date.today()}'")
        .partitionBy("extract_date")
        .save(target_path))

    count = df.count()
    log.info("✓ %d lignes écrites dans %s", count, target_path)
    return count


def main():
    log.info("=== Démarrage du job Bronze ===")

    # Lancement de la session Spark
    spark = (
        SparkSession.builder
        .appName("bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Traitement de la table employees
    n_emp = ingest_table(spark, "hr.employees", f"{BRONZE_BASE}/employees")
    # Traitement de la table activities
    n_act = ingest_table(spark, "ops.activities", f"{BRONZE_BASE}/activities")

    log.info("=== Bronze terminé : %d employés, %d activités ===", n_emp, n_act)
    spark.stop()


if __name__ == "__main__":
    main()