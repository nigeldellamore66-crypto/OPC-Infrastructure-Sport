"""
Point d'entrée du simulateur.

Lit la variable SIMULATOR_MODE et lance le bon mode :
  - bootstrap            : charge le RH + génère l'historique, puis s'arrête
  - live                 : insère une activité aléatoire toutes les X minutes
  - bootstrap_then_live   : bootstrap puis bascule en live (défaut)
"""

import os
import sys
import time
import random
import logging
import db
import bootstrap

# Configuration des logs 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("simulator")


def run_live():
    """Insère une activité live aléatoire toutes les SIMULATOR_INTERVAL_SECONDS. Défini dans le docker-compose"""
    # Intervalle de génération d'activités
    interval = int(os.getenv("SIMULATOR_INTERVAL_SECONDS", "300"))
    logger.info("=== Mode LIVE === (intervalle: %ds)", interval)

    # Récupère les employés dans la base
    employees = db.fetch_employees()
    if not employees:
        logger.error("Aucun employé en base. Lance d'abord le bootstrap.")
        return

    while True:
        # Sélectionne un employé aléatoire et génère une activité live
        emp = random.choice(employees)
        activity = bootstrap.generate_activity(
            emp["employee_id"], emp["hire_date"], "live"
        )
        # Insère l'activité dans la base
        db.insert_activities_bulk([activity])
        logger.info(
            "Activité live: employé=%d sport=%s",
            activity["employee_id"], activity["sport_type"],
        )
        time.sleep(interval)


def main():
    mode = os.getenv("SIMULATOR_MODE", "bootstrap_then_live").strip().lower()
    logger.info("Démarrage du simulateur, mode=%s", mode)

    db.wait_for_postgres()

    # Lance le simulateur selon le mode choisi
    if mode == "bootstrap":
        bootstrap.run()
    elif mode == "live":
        run_live()
    elif mode == "bootstrap_then_live":
        bootstrap.run()
        run_live()
    else:
        logger.error("Mode inconnu: %s", mode)
        sys.exit(1)


if __name__ == "__main__":
    main()