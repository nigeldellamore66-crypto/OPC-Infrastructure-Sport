"""
Couche Silver du médaillon.

Lit les tables Bronze, enrichit (Google Maps + calculs dérivés),
écrit en Delta sur s3a://lakehouse/silver/.
"""

import logging
import os
import sys
from datetime import date
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("silver")

# Défintion des chemins des données Delta
BRONZE_BASE = "s3a://lakehouse/bronze"
SILVER_BASE = "s3a://lakehouse/silver"
# Définition des variables API Google et adresse
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
OFFICE_ADDRESS = os.getenv("OFFICE_ADDRESS", "1362 Av. des Platanes, 34970 Lattes")
GOOGLE_MAPS_URL = "https://maps.googleapis.com/maps/api/distancematrix/json"

def fetch_distance(home_address):
    """
    Appelle Google Maps pour 1 trajet domicile → bureau.
    """
     # Retourne aucune distance si clé manquante
    if not GOOGLE_MAPS_API_KEY:
        return (None, "no_api_key")

    try:
        response = requests.get( # Requête sur l'API google pour calculer la distance
            GOOGLE_MAPS_URL,
            params={
                "origins": home_address,
                "destinations": OFFICE_ADDRESS,
                "mode": "driving",
                "units": "metric",
                "key": GOOGLE_MAPS_API_KEY,
            },
            timeout=10,
        )
        data = response.json()

        # Renvoie une erreur si le status global de la requête est différent de OK
        if data.get("status") != "OK":
            log.warning("Google Maps error pour '%s': %s | error_message: %s", 
                        home_address, data.get("status"), data.get("error_message", "none"))
            return (None, "error")

        element = data["rows"][0]["elements"][0] # Sélectionne le status du trajet dans le dictionnaire de la requête
        if element.get("status") != "OK": # Renvoie une erreur si status trajet différent de requête
            return (None, "not_found")

        distance_m = element["distance"]["value"] # Récupère la distance en mètres dans le dictionnaire de la requête
        return (round(distance_m / 1000.0, 2), "ok") # Renvoie la distance en Km et un status ok

    except Exception as e:
        log.exception("Exception lors de l'appel Google Maps pour '%s': %s", home_address, e)
        return (None, "error")
    
def transform_employees(spark):
    """Lit bronze/employees, enrichit avec Google Maps + calculs, écrit silver/employees."""
    log.info("=== Transformation Silver: employees ===")

    # Définition de la date du jour et des chemins des données bronze/silver
    today = date.today()
    bronze_path = f"{BRONZE_BASE}/employees"
    silver_path = f"{SILVER_BASE}/employees"

    # Lecture des données bronze au format spark
    df_bronze = (
        spark.read.format("delta").load(bronze_path)
        .filter(F.col("extract_date") == today)
    )
    employees = df_bronze.collect() # transforme le dataframe Spark en liste Python
    log.info("Lu %d employés depuis Bronze", len(employees))

    # Enrichit la liste python 
    enriched_rows = []
    for emp in employees:
        distance_km, status = fetch_distance(emp["home_address"]) # Récupère la distance de l'adresse de l'employé
        enriched_rows.append({
            "employee_id": emp["employee_id"],
            "full_name": f"{emp['first_name']} {emp['last_name']}",
            "first_name": emp["first_name"],
            "last_name": emp["last_name"],
            "birth_date": emp["birth_date"],
            "business_unit": emp["business_unit"],
            "hire_date": emp["hire_date"],
            "gross_salary": float(emp["gross_salary"]),
            "contract_type": emp["contract_type"],
            "paid_leave_days": emp["paid_leave_days"],
            "home_address": emp["home_address"],
            "transport_mode": emp["transport_mode"],
            "home_to_office_km": distance_km,
            "distance_check_status": status,
        })

    # Définition du schéma de reconstruction spark
    schema = StructType([
        StructField("employee_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("business_unit", StringType(), True),
        StructField("hire_date", StringType(), True),
        StructField("gross_salary", DoubleType(), True),
        StructField("contract_type", StringType(), True),
        StructField("paid_leave_days", StringType(), True),
        StructField("home_address", StringType(), True),
        StructField("transport_mode", StringType(), True),
        StructField("home_to_office_km", DoubleType(), True),
        StructField("distance_check_status", StringType(), True),
    ])

    # Reconstruction spark et enrichissement des colonnes
    df_silver = (
        spark.createDataFrame(enriched_rows, schema=schema)
        .withColumn("age", F.floor(F.months_between(F.current_date(), F.to_date("birth_date")) / 12)) # Convertit de la date en année 
        .withColumn("seniority_years", F.floor(F.months_between(F.current_date(), F.to_date("hire_date")) / 12))
        .withColumn("employee_id", F.col("employee_id").cast("int"))
        .withColumn("birth_date", F.to_date("birth_date"))
        .withColumn("hire_date", F.to_date("hire_date"))
        .withColumn("paid_leave_days", F.col("paid_leave_days").cast("int"))
        .withColumn("extract_date", F.lit(today))
        .withColumn("extract_ts", F.current_timestamp())
    )

    # Ecriture du dataframe enrichi au format delta dans silver
    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"extract_date = '{today}'")
        .partitionBy("extract_date")
        .save(silver_path))

    log.info("✓ %d employés écrits dans %s", df_silver.count(), silver_path)

def transform_activities(spark):
    """Lit bronze/activities + silver/employees, enrichit, écrit silver/activities."""
    log.info("=== Transformation Silver: activities ===")

    # Récupère la date du jour et les chemin bronze/silver
    today = date.today()
    bronze_path = f"{BRONZE_BASE}/activities"
    silver_emp_path = f"{SILVER_BASE}/employees"
    silver_act_path = f"{SILVER_BASE}/activities"

    # Lit bronze activities au format spark
    df_act = (
        spark.read.format("delta").load(bronze_path)
        .filter(F.col("extract_date") == today)
    )
    log.info("Lu %d activités depuis Bronze", df_act.count())

    # Lit silver employees au format spark
    df_emp = (
        spark.read.format("delta").load(silver_emp_path)
        .filter(F.col("extract_date") == today)
        .select("employee_id", "full_name", "business_unit")
    )

    # Assemblement de activities silver
    df_silver = (
        df_act.alias("a")
        .join(df_emp.alias("e"), on="employee_id", how="left") # jointures avec employees
        .select( # Sélection des colonnes et enrichissement
            F.col("a.activity_id").alias("activity_id"),
            F.col("a.employee_id").alias("employee_id"),
            F.col("e.full_name").alias("full_name"),
            F.col("e.business_unit").alias("business_unit"),
            F.col("a.sport_type").alias("sport_type"),
            F.col("a.start_at").alias("start_at"),
            F.col("a.end_at").alias("end_at"),
            (F.col("a.distance_m") / 1000.0).alias("distance_km"), # conversion en mètres
            (F.col("a.duration_s") / 60.0).alias("duration_min"), # conversion en minutes
            F.col("a.comment").alias("comment"),
            F.col("a.source").alias("source"),
        )
        .withColumn(
            "pace_min_per_km",
            F.when( # si sport avec distance on calcule la vitesse
                (F.col("distance_km").isNotNull()) & (F.col("distance_km") > 0),
                F.round(F.col("duration_min") / F.col("distance_km"), 2),
            ).otherwise(None),
        )
        .withColumn("extract_date", F.lit(today))
        .withColumn("extract_ts", F.current_timestamp())
    )

    # Ecriture de activities silver au format delta
    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"extract_date = '{today}'")
        .partitionBy("extract_date")
        .save(silver_act_path))

    log.info("✓ %d activités écrites dans %s", df_silver.count(), silver_act_path)

def main():
    log.info("=== Démarrage du job Silver ===")

    # Démarrage de la session spark
    spark = (
        SparkSession.builder
        .appName("silver")
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

    # transformation et écriture table employees
    transform_employees(spark)
    # transformation et écriture table activities
    transform_activities(spark)

    log.info("=== Silver terminé ===")
    spark.stop()

if __name__ == "__main__":
    main()