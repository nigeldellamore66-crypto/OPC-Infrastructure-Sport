"""
Charge le fichier RH et génère un historique 12 mois.
"""

import logging
import random
from datetime import datetime, timedelta, date, time
import pandas as pd
import db

logger = logging.getLogger(__name__)
random.seed(42)

SPORTS = {
    "Course à pied": {"distance_range": (3, 15),  "speed_range": (8, 14)},
    "Vélo":          {"distance_range": (10, 80), "speed_range": (18, 30)},
    "Marche":        {"distance_range": (2, 10),  "speed_range": (4, 6)},
    "Randonnée":     {"distance_range": (5, 25),  "speed_range": (3, 5)},
    "Natation":      {"distance_range": (0.5, 4), "speed_range": (2, 4)},
    "Yoga":          {"duration_range": (30, 90)},
    "Fitness":       {"duration_range": (30, 90)},
    "Escalade":      {"duration_range": (45, 180)},
}

def load_rh_file(path="/data/donnees_rh.xlsx"):
    """Lit le fichier Excel RH et retourne une liste de dicts."""
    df = pd.read_excel(path)
    logger.info("Fichier RH chargé: %d lignes", len(df))

    employees = []
    for _, row in df.iterrows(): # Convertit chaque ligne du dataframe en dictionnaire python pour insertion
        employees.append({
            "employee_id":     int(row["ID salarié"]),
            "last_name":       str(row["Nom"]).strip(),
            "first_name":      str(row["Prénom"]).strip(),
            "birth_date":      row["Date de naissance"].date(),
            "business_unit":   str(row["BU"]).strip(),
            "hire_date":       row["Date d'embauche"].date(),
            "gross_salary":    float(row["Salaire brut"]),
            "contract_type":   str(row["Type de contrat"]).strip(),
            "paid_leave_days": int(row["Nombre de jours de CP"]),
            "home_address":    str(row["Adresse du domicile"]).strip(),
            "transport_mode":  str(row["Moyen de déplacement"]).strip(),
        })
    return employees

def generate_activity(employee_id, hire_date, mode):
    """Génère une activité aléatoire cohérente pour un employé."""

    sport = random.choice(list(SPORTS.keys())) # Choisi un sport aléatoire pour une employé donné
    spec = SPORTS[sport]

    if "distance_range" in spec: # Si distance renseignée, sport avec distance
        min_km, max_km = spec["distance_range"]
        distance_km = round(random.uniform(min_km, max_km), 2)
        min_kmh, max_kmh = spec["speed_range"]
        speed_kmh = random.uniform(min_kmh, max_kmh)
        # Calcul la durée et distance de l'activité à partir des données randomisées des ranges
        duration_s = int((distance_km / speed_kmh) * 3600)
        distance_m = int(distance_km * 1000)
    else: # Sinon sport sans distance
        min_min, max_min = spec["duration_range"]
        duration_s = random.randint(min_min, max_min) * 60
        distance_m = None
    # Calculer les dates selon le mode de simulation live/bootstrap 
    if mode == "live":
        end_at = datetime.now()
        start_at = end_at - timedelta(seconds=duration_s)
    else:
        days_ago = random.randint(1, 365) # Nombre de jours aléatoires dans l'année passée
        day = date.today() - timedelta(days=days_ago) # Jour de l'activité
        if day < hire_date: # Cohérence avec la date d'embauche
            day = hire_date + timedelta(days=1)
        hour = random.randint(6, 20)
        minute = random.randint(0, 59)
        start_at = datetime.combine(day, time(hour=hour, minute=minute))
        end_at = start_at + timedelta(seconds=duration_s)

    return {
        "employee_id": employee_id,
        "sport_type":  sport,
        "start_at":    start_at,
        "end_at":      end_at,
        "distance_m":  distance_m,
        "duration_s":  duration_s,
        "comment":     None,
        "source":      mode,
    }

def run():
    """Mode bootstrap : charge le RH puis génère l'historique 12 mois."""
    logger.info("=== Mode BOOTSTRAP ===")

    # Vérifie l'existence des employés en base
    if db.employees_exist():
        logger.info("Employés déjà en base, skip chargement RH.")
    else:
        employees = load_rh_file()
        db.insert_employees(employees)

    # Vérifie l'existence des activités dans l'historique
    if db.historical_activities_exist():
        logger.info("Historique déjà en base, skip génération.")
        return

    employees = db.fetch_employees() # Lit les employés depuis la base si déja existants
    all_activities = []
    for emp in employees: # Pour chaque employé
        n = random.randint(0, 50) # Un nombre aléatoire d'activités
        for _ in range(n): # Génére le nombre d'activités sélectionné pour chaque employé
            activity = generate_activity(emp["employee_id"], emp["hire_date"], "historical")
            all_activities.append(activity)

    logger.info("Total activités générées: %d", len(all_activities))
    db.insert_activities_bulk(all_activities) # Insère les activités dans la base
    logger.info("Bootstrap terminé.")