# Sport Data Solution — POC Avantages Sportifs

POC de pipeline data end-to-end pour calculer les avantages sportifs des
salariés (prime de 5% + jours bien-être) à partir d'activités sportives
simulées.

## Architecture

```
Postgres (RH + activités)
    │
    │ Debezium CDC
    ▼
Redpanda (topics)
    │
    ├──► Spark Streaming ──► Slack
    │
    └──► Spark Batch ──► MinIO/Delta (bronze→silver→gold) ──► Postgres analytics ──► Power BI
                              ▲
                              │
                         Google Maps API
```

## Prérequis

- Docker Desktop ou Docker Engine + Docker Compose v2
- 16 Go de RAM minimum 
- Ports libres sur la machine hôte: 5432, 8080, 8081, 8082, 8083, 8088,
  9000, 9001, 9090, 3000, 19092

## Démarrage

```bash
# 1. Cloner et préparer l'environnement
cp .env.example .env
# Editer .env et générer les clés Airflow:
python -c "import secrets; print(secrets.token_hex(16))"   # → AIRFLOW_SECRET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # → AIRFLOW_FERNET_KEY

# 2. Démarrer la stack
docker compose up -d

# 3. Vérifier que tout est UP (peut prendre 2-3 minutes la première fois à cause du build Spark)
docker compose ps

# 4. Enregistrer le connecteur Debezium
./scripts/register-debezium.sh
```

## Interfaces web

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8088 | admin / cf .env |
| Redpanda Console | http://localhost:8080 | — |
| Spark Master | http://localhost:8081 | — |
| MinIO Console | http://localhost:9001 | minioadmin / cf .env |
| Grafana | http://localhost:3000 | admin / cf .env |
| Prometheus | http://localhost:9090 | — |

## Structure du projet

```
sport-data-poc/
├── docker-compose.yml          ← orchestration de toute la stack
├── .env.example                ← variables à recopier en .env
├── docker/
│   ├── spark/                  ← image Spark + JARs Delta/Kafka/S3
│   └── simulator/              ← simulateur Strava
├── sql/init/                   ← DDL Postgres (auto-exécuté au 1er démarrage)
├── connectors/                 ← config Debezium
├── jobs/                       ← jobs PySpark (à venir)
├── airflow/dags/               ← DAGs Airflow (à venir)
├── monitoring/                 ← Prometheus + Grafana
└── scripts/                    ← scripts utilitaires
```
