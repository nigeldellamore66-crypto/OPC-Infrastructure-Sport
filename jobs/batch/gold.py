"""
Couche Gold du médaillon.

Lit Silver + dim_parameters depuis Postgres, calcule les éligibilités
prime sportive et 5 jours bien-être, écrit le résultat en Delta.
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
log = logging.getLogger("gold")


# Définition des chemins silver/gold MiniO
SILVER_BASE = "s3a://lakehouse/silver"
GOLD_BASE = "s3a://lakehouse/gold"
# Définition des paramètres de connection postgres
JDBC_URL = "jdbc:postgresql://postgres:5432/sportdata"
JDBC_PROPERTIES = {
    "user":     os.getenv("POSTGRES_USER", "sportadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
    "driver":   "org.postgresql.Driver",
}

def fetch_parameters(spark):
    """
    Lit la dernière ligne de analytics.dim_parameters.
    """
    #Lecture spark de la table dim_parameters
    df = spark.read.jdbc(
        url=JDBC_URL,
        table="(SELECT * FROM analytics.dim_parameters ORDER BY snapshot_date DESC LIMIT 1) AS p",
        properties=JDBC_PROPERTIES,
    )
    # Transforme les données spark en dict python
    row = df.collect()[0]
    params = {
        "premium_rate":         float(row["premium_rate"]),
        "wellbeing_threshold":  int(row["wellbeing_threshold"]),
        "wellbeing_days":       int(row["wellbeing_days"]),
        "max_distance_walk_km": int(row["max_distance_walk_km"]),
        "max_distance_bike_km": int(row["max_distance_bike_km"]),
    }
    log.info("Paramètres en vigueur: %s", params)
    return params

def calculate_eligibility(spark, params):
    """
    Calcule l'éligibilité prime + bien-être pour chaque employé.
    """

    log.info("=== Calcul des éligibilités ===")
    today = date.today()

    # Lit les données employees silver enrichis
    df_emp = (
        spark.read.format("delta").load(f"{SILVER_BASE}/employees")
        .filter(F.col("extract_date") == today)
    )

    # Lit les données activities silver enrichis
    df_act = (
        spark.read.format("delta").load(f"{SILVER_BASE}/activities")
        .filter(F.col("extract_date") == today)
        .filter(F.col("start_at") >= F.date_sub(F.current_date(), 365))
    )

    # Compte le nombre d'activités par employés
    df_counts = (
        df_act.groupBy("employee_id")
        .agg(F.count("*").alias("activities_count_12m"))
    )

    df = (
        df_emp.alias("e")
        .join(df_counts.alias("c"), on="employee_id", how="left") # Left join sur employee_id pour garder ceux qui n'ont pas d'activities
        .withColumn(
            "activities_count_12m",
            F.coalesce(F.col("activities_count_12m"), F.lit(0)) # remplace les activities NULL par 0
        )
        .withColumn(
            "is_eligible_premium", # vérifie si l'employé est éligibile à la prime
            (F.col("transport_mode") == "Marche/running") # si le mode de transport est la marche
              & (F.col("home_to_office_km").isNotNull())
              & (F.col("home_to_office_km") <= params["max_distance_walk_km"]) # distance maximale de marche en km
            |
            (F.col("transport_mode") == "Vélo/Trottinette/Autres") # si le mode de transport est un véhicule
              & (F.col("home_to_office_km").isNotNull())
              & (F.col("home_to_office_km") <= params["max_distance_bike_km"]) # distance maximale de vélo en km
        )
        .withColumn(
            "premium_amount", # calcule la quantité de la prime
            F.when(
                F.col("is_eligible_premium"), # si l'employé est éligible à la prime
                F.round(F.col("gross_salary") * params["premium_rate"], 2) # multiplie le salaire brut annuel x le taux de prime défini
            ).otherwise(F.lit(0.0)) # sinon 0
        )
        .withColumn(
            "is_eligible_wellbeing", # vérifie si l'employé est éligible aux jours de congès bien-être
            F.col("activities_count_12m") >= params["wellbeing_threshold"] # nombre d'activités supérieur au seuil défini
        )
        .withColumn(
            "wellbeing_days", # si éligible aux jours bien être on lui attribue le nombre de jours définis
            F.when(
                F.col("is_eligible_wellbeing"),
                F.lit(params["wellbeing_days"])
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "wellbeing_cost", # calcule le coût des jours bien être
            F.when(
                F.col("is_eligible_wellbeing"), # si éligible aux jours bien-être
                F.round((F.col("gross_salary") / 365.0) * params["wellbeing_days"], 2) # salaire brut annuel / 365 x 5 jours bien-être
            ).otherwise(F.lit(0.0)) # sinon 0
        )
        .withColumn(
            "total_cost", # coût total = coût prime + coût bien-être
            F.round(F.col("premium_amount") + F.col("wellbeing_cost"), 2)
        )
        .withColumn("snapshot_date", F.lit(today))
        .select(
            "snapshot_date",
            "employee_id",
            "full_name",
            "business_unit",
            "contract_type",
            "gross_salary",
            "transport_mode",
            "home_to_office_km",
            "distance_check_status",
            "is_eligible_premium",
            "premium_amount",
            "activities_count_12m",
            "is_eligible_wellbeing",
            "wellbeing_days",
            "wellbeing_cost",
            "total_cost",
        )
    )

    return df

def write_gold(df):
    """Écrit le DataFrame employee_eligibility en Delta au chemin Gold """
    target_path = f"{GOLD_BASE}/employee_eligibility"
    today = date.today()

    # Ecriture delta gold pour médaillon
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_date = '{today}'")
        .partitionBy("snapshot_date")
        .save(target_path))

    log.info("✓ %d lignes éligibilité écrites dans %s", df.count(), target_path)

     # Export Postgres pour Power BI
    (df.write
        .mode("overwrite")
        .option("truncate", "true") # pas d'historique des snapshots
        .jdbc(url=JDBC_URL, table="analytics.fct_employee_eligibility", properties=JDBC_PROPERTIES))

    log.info("Lignes éligibilité publiées vers Postgres analytics.fct_employee_eligibility")

def main():
    log.info("=== Démarrage du job Gold ===")

    # Création session spark pour lire les données silver et écrire le gold
    spark = (
        SparkSession.builder
        .appName("gold")
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

    # Récupère les paramètres d'éligibilité définis dans la table postgre
    params = fetch_parameters(spark)
    # Récupère les données silver et calcule l'éligibilité des employés à la prime et aux jours bien-être
    df = calculate_eligibility(spark, params)
    # Ecrit le dataframe employee_eligibility dans gold
    write_gold(df)

    log.info("=== Gold terminé ===")
    spark.stop()


if __name__ == "__main__":
    main()