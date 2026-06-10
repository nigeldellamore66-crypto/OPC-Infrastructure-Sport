"""
DAG d'orchestration du pipeline médaillon.

Execute : bronze → silver → gold
Fréquence : quotidienne
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# Paramètres par défaut appliqué sur toutes les tâches
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Configuration spark pour chaque batch du médaillon
SPARK_CONF = {
    "spark.executor.memory": "512m",
    "spark.driver.memory": "512m",
    "spark.cores.max": "1",
    "spark.driver.host": "sport-airflow", # Nom DNS dans docker
}


with DAG(
    dag_id="sport_data_pipeline",
    description="Pipeline médaillon Bronze → Silver → Gold",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 5, 1), # Date de départ du DAG
    catchup=False, # Ne rattrape pas toutes les itérations passées
    tags=["sport-data", "medaillon"],
) as dag:
    # Spark submit du batch bronze
    bronze = SparkSubmitOperator(
        task_id="bronze",
        application="/opt/airflow/jobs/batch/bronze.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        verbose=True,
    )
    # Spark submit du batch silver
    silver = SparkSubmitOperator(
        task_id="silver",
        application="/opt/airflow/jobs/batch/silver.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        verbose=True,
    )
    # Spark submit du batch gold
    gold = SparkSubmitOperator(
        task_id="gold",
        application="/opt/airflow/jobs/batch/gold.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        verbose=True,
    )
    # Enchaînement du DAG
    bronze >> silver >> gold