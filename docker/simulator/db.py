"""
Helpers pour parler à Postgres.
"""

import os
import time
import logging
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

def _conn_params():
    """Retourne les paramètres de connexion lus depuis les variables d'environnement."""
    return {
        "host":     os.getenv("POSTGRES_HOST", "postgres"),
        "port":     int(os.getenv("POSTGRES_PORT", "5432")),
        "user":     os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "dbname":   os.getenv("POSTGRES_DB"),
    }

def wait_for_postgres(max_retries=30, delay=2):
    """Attend que Postgres soit prêt à accepter des connexions."""
    for attempt in range(1, max_retries + 1):
        try: # Essaye la connection puis la ferme immédiatement
            conn = psycopg2.connect(**_conn_params())
            conn.close()
            logger.info("Postgres est prêt (essai %d)", attempt)
            return
        except psycopg2.OperationalError:
            logger.info("Postgres pas prêt, retry dans %ds (essai %d/%d)", delay, attempt, max_retries)
            time.sleep(delay)
    raise RuntimeError("Postgres n'est pas devenu disponible après %d essais" % max_retries)

def insert_employees(employees):
    """Insère une liste d'employés dans hr.employees. Ignore les doublons."""
    conn = psycopg2.connect(**_conn_params()) # Ouvre la connection postgres
    try:
        with conn.cursor() as cur: # Ouvre un curseur pour l'éxécution des requêtes SQL
            execute_values(  # Fonction permettant d'insérer plusieurs lignes en une seule requête
                cur,
                """
                INSERT INTO hr.employees (
                    employee_id, last_name, first_name, birth_date,
                    business_unit, hire_date, gross_salary, contract_type,
                    paid_leave_days, home_address, transport_mode
                ) VALUES %s
                ON CONFLICT (employee_id) DO NOTHING
                """,
                [
                    (
                        e["employee_id"], e["last_name"], e["first_name"], e["birth_date"],
                        e["business_unit"], e["hire_date"], e["gross_salary"], e["contract_type"],
                        e["paid_leave_days"], e["home_address"], e["transport_mode"],
                    )
                    for e in employees  # Créer une liste de tuple avec la liste d'employés qui seront insérés dans %s
                ],
            )
        conn.commit() # Valide la requête
        logger.info("Insertion de %d employés terminée", len(employees))
    finally: # On ferme la connection quoi qu'il arrive
        conn.close()

def insert_activities_bulk(activities, batch_size=500):
    """Insère une liste d'activités par batches."""
    conn = psycopg2.connect(**_conn_params()) # Ouvre la connection postgres
    try:
        with conn.cursor() as cur: # Ouvre un curseur pour l'éxécution des requêtes SQL
            for i in range(0, len(activities), batch_size): # Boucle d'insertion en batch
                batch = activities[i:i + batch_size] # Découpe les batchs en commencant à 0
                execute_values(
                    cur,
                    """
                    INSERT INTO ops.activities (
                        employee_id, sport_type, start_at, end_at,
                        distance_m, duration_s, comment, source
                    ) VALUES %s
                    """,
                    [
                        (
                            a["employee_id"], a["sport_type"], a["start_at"], a["end_at"],
                            a["distance_m"], a["duration_s"], a["comment"], a["source"],
                        )
                        for a in batch
                    ],
                )
                conn.commit()
                logger.info("Bulk inséré: %d / %d", i + len(batch), len(activities))
    finally:
        conn.close() # On ferme la connection quoi qu'il arrive

def employees_exist():
    """Retourne True si la table hr.employees contient au moins une ligne."""
    conn = psycopg2.connect(**_conn_params())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM hr.employees LIMIT 1)") # Requête qui retourne un booléen
            return cur.fetchone()[0]
    finally:
        conn.close()

def historical_activities_exist():
    """Retourne True s'il y a déjà des activités avec source='historical'."""
    conn = psycopg2.connect(**_conn_params())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM ops.activities WHERE source = 'historical' LIMIT 1)" # Requête qui retourne un booléen
            )
            return cur.fetchone()[0]
    finally:
        conn.close()

def fetch_employees():
    """Récupère tous les employés depuis la base (id, prénom, nom, date d'embauche)."""
    conn = psycopg2.connect(**_conn_params())
    try:
        with conn.cursor() as cur:
            cur.execute( # Requête select sur la table employees
                """
                SELECT employee_id, first_name, last_name, hire_date
                FROM hr.employees
                ORDER BY employee_id
                """
            )
            rows = cur.fetchall()
            result = []
            for row in rows:
                result.append({
                    "employee_id": row[0],
                    "first_name":  row[1],
                    "last_name":   row[2],
                    "hire_date":   row[3],
                })
            return result
    finally:
        conn.close()