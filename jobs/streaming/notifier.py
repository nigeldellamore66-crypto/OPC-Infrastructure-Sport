"""
Helpers pour envoyer un message Slack et faire une recherche employé.

Importé par le job Spark Streaming `slack_notifier.py`.
"""

import json
import logging
import os
import urllib.request
import urllib.error
from typing import Optional
import psycopg2

log = logging.getLogger(__name__)


# Lookup Postgres

_pg_conn = None  # variable de connection globale utilisée par worker Python


def _get_pg_conn():
    """Ouvre une connexion Postgres."""
    global _pg_conn # connection globale
    if _pg_conn is None or _pg_conn.closed:
        _pg_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "sportadmin"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            dbname=os.getenv("POSTGRES_DB", "sportdata"),
        )
        _pg_conn.autocommit = True
    return _pg_conn # Retourne la connection crée ou deja existante


def fetch_employee_name(employee_id: int) -> str:
    """Retourne 'Prénom Nom' ou 'Employé #<id>' si introuvable."""
    try:
        conn = _get_pg_conn() # Récupère la connection
        with conn.cursor() as cur:
            cur.execute(
                "SELECT first_name, last_name FROM hr.employees WHERE employee_id = %s",
                (employee_id,),
            )
            row = cur.fetchone() # On récupère une ligne (prénom,nom) ou None
            if row: # Si ce n'est pas None on retourne la ligne
                return f"{row[0]} {row[1]}"
    except Exception as e: # Si erreur on relance la connection sans crash du job spark
        log.exception("Erreur lookup employé %s: %s", employee_id, e)
        # Force la réouverture au prochain appel
        global _pg_conn
        _pg_conn = None
    return f"Employé #{employee_id}" # Renvoie l'ID employé  si il n'existe pas en base ou une exception


# Formatage du message Slack

# Dictionnaire: emojis par type d'activité
EMOJI = {
    "Course à pied": "🏃",
    "Vélo":          "🚴",
    "Marche":        "🚶",
    "Randonnée":     "🥾",
    "Natation":      "🏊",
    "Escalade":      "🧗",
    "Yoga":          "🧘",
    "Fitness":       "💪",
    "Tennis":        "🎾",
}

# Dictionnaire: Verbes en français selon l'activité
VERB = {
    "Course à pied": "courir",
    "Vélo":          "rouler",
    "Marche":        "marcher",
    "Randonnée":     "randonner",
    "Natation":      "nager",
}


def format_slack_message(full_name: str, sport_type: str,
                        distance_m: Optional[int], duration_s: int,
                        comment: Optional[str]) -> str:
    """Compose un message à lenvoyer dans slack."""
    # Renvoie un emoji selon le type de sport 
    emoji = EMOJI.get(sport_type, "🏅")
    duration_min = duration_s // 60

    # Distinction sport avec distance et sans
    if distance_m and distance_m > 0:
        distance_km = distance_m / 1000.0
        # Format français de distance avec virgule
        dist_str = f"{distance_km:.1f}".replace(".", ",")
        # Sélection du verbe approprié selon le type de sport
        verb = VERB.get(sport_type, "faire")
        if verb == "faire":
            action = f"faire {duration_min} min de {sport_type.lower()}"
            base = f"Bravo {full_name} ! Tu viens de {action} sur {dist_str} km !"
        else:
            base = f"Bravo {full_name} ! Tu viens de {verb} {dist_str} km en {duration_min} min !"
    else:
        # Suppression devant voyelle: "60 min d'escalade" plutôt que "de escalade"
        sport_lower = sport_type.lower()
        connector = "d'" if sport_lower[0] in "aeiouéèàâ" else "de "
        base = f"Bravo {full_name} ! {duration_min} min {connector}{sport_lower} bouclées !"

    # Formatage du message avec l'emoji
    msg = f"{base} {emoji}"
    if comment:
        msg += f' ("{comment}")'
    return msg


# Envoi Slack (avec mode MOCK)

def send_to_slack(message: str):
    """Envoie sur Slack via webhook, ou log si la variable est en mode MOCK."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "MOCK").strip()

    # Si webhook slack non configuré on log seulement le message
    if webhook_url in ("", "MOCK"):
        log.info("[SLACK MOCK] %s", message)
        return

    try:
        # Construction corps de requête slack
        corps = json.dumps({"text": message}).encode("utf-8")

        req = urllib.request.Request(
            webhook_url, # URL cible    
            data=corps, # Corps requête
            headers={"Content-Type": "application/json"}, # Annonce du JSON a Slack
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp: # éxecute la requête avec timeout pour ne pas bloquer spark
            if resp.status >= 400: # log erreur
                log.error("Slack a retourné %d", resp.status)
            else: # log succès
                log.info("Slack OK: %s", message[:80])
    except urllib.error.URLError as e:
        log.error("Erreur réseau Slack: %s", e)
    except Exception as e:
        log.exception("Erreur inattendue Slack: %s", e)
