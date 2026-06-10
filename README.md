# Sport Data Solution — POC Avantages Sportifs

POC de pipeline data **end-to-end** pour calculer automatiquement les avantages sportifs des salariés à partir d'activités sportives simulées :

- **Prime mobilité** — 5 % du salaire annuel brut si le trajet domicile–bureau est réalisable activement (≤ 15 km à pied ou ≤ 25 km à vélo).
- **Jours bien-être** — 5 jours de congé supplémentaires pour les salariés ayant déclaré ≥ 15 activités sportives sur 12 mois.

Le projet démontre une chaîne data complète : capture temps réel (CDC), traitement streaming **et** batch, lakehouse médaillon, restitution BI, tests de qualité et monitoring.

---

## Résultats clés (jeu de données de démo)

| Indicateur | Valeur |
|---|---|
| Employés traités | 161 |
| Coût total annuel | 255 780 € (3,2 % de la masse salariale) |
| Primes mobilité attribuées | 68 (42 %) |
| Congés bien-être attribués | 120 (75 %) |
| Employés géocodés | 161 / 161 |
| Activités sportives | 4 160 |

---

## Architecture

```
Postgres (RH + activités)
    │
    │ Debezium CDC (lecture du WAL, plugin pgoutput)
    ▼
Redpanda (topics Kafka : cdc.hr.employees, cdc.ops.activities)
    │
    ├──► Spark Streaming ──► Slack          (temps réel : notification à chaque activité)
    │
    └──► Spark Batch ──► MinIO / Delta Lake ──► Postgres analytics ──► Power BI
                         (bronze → silver → gold)
                              ▲
                              │
                         Google Maps API (géocodage + distances)

Orchestration : Airflow (DAG @daily : bronze >> silver >> gold)
Qualité       : Great Expectations (conteneur dédié)
Monitoring    : Prometheus + Grafana
```

**Deux flux complémentaires :**
- **Temps réel** — un INSERT en base est capté par Debezium, publié sur Redpanda, consommé par Spark Streaming qui notifie Slack en quelques secondes.
- **Batch** — le pipeline médaillon raffine la donnée (Bronze brut → Silver nettoyé/enrichi → Gold table de faits), orchestré quotidiennement par Airflow.

**Principe d'architecture clé :** l'historique vit dans **Delta/MinIO** (source de vérité, time travel) ; **Postgres analytics** n'est qu'une vitrine de service pour la BI (dernier état uniquement, réécrit à chaque run).

---

## Stack technique

| Catégorie | Outils |
|---|---|
| Base source | PostgreSQL 16 |
| CDC | Debezium 2.7 (plugin `pgoutput`) |
| Streaming | Redpanda 24.2 (compatible Kafka) |
| Traitement | Apache Spark 3.5.3 (PySpark, Python 3.8) |
| Stockage objet | MinIO + Delta Lake 3.2.1 |
| Orchestration | Apache Airflow 2.10.3 |
| Qualité | Great Expectations 0.18.15 |
| Monitoring | Prometheus + Grafana 11.3 |
| Restitution | Power BI Desktop |
| Conteneurisation | Docker Compose (11 services) |

---

## Prérequis

- Docker Desktop ou Docker Engine + Docker Compose v2
- 16 Go de RAM minimum
- Ports libres sur la machine hôte : `5433`, `8080`, `8081`, `8082`, `8083`, `8088`, `9000`, `9001`, `9090`, `3000`, `19092`

> **Postgres est exposé sur le port `5433`** côté hôte (et non 5432) pour éviter un conflit avec une éventuelle instance PostgreSQL locale. En interne Docker, les services communiquent toujours via `postgres:5432`.

---

## Démarrage

```bash
# 1. Cloner et préparer l'environnement
cp .env.example .env

# Générer les clés Airflow et les reporter dans .env :
python -c "import secrets; print(secrets.token_hex(16))"                                    # → AIRFLOW_SECRET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # → AIRFLOW_FERNET_KEY

# 2. Démarrer la stack
docker compose up -d

# 3. Vérifier que tout est UP (2-3 min au premier lancement à cause du build Spark)
docker compose ps

# 4. Enregistrer le connecteur Debezium
curl.exe -X POST -H "Content-Type: application/json" \
  --data "@connectors/postgres-source.json" \
  http://localhost:8083/connectors

# 5. Vérifier que le connecteur tourne
curl.exe http://localhost:8083/connectors/postgres-cdc-source/status
```

Une fois la stack démarrée, le **simulateur Strava** génère des activités sportives en continu, qui déclenchent le flux CDC temps réel (notifications Slack).

---

## Lancer le pipeline batch

Le pipeline médaillon est orchestré par Airflow. Pour le déclencher :

1. Ouvrir Airflow : http://localhost:8088
2. Activer le DAG `sport_data_pipeline`
3. Le déclencher manuellement (**▶ Trigger DAG**) ou attendre le run quotidien

Le DAG enchaîne `bronze >> silver >> gold` via `SparkSubmitOperator`. À la fin, la table de faits `analytics.fct_employee_eligibility` est disponible dans Postgres pour Power BI.

---

## Tests de qualité (Great Expectations)

Les tests de qualité tournent dans un **conteneur dédié** (profil `tests`, ne démarre pas avec un `up` normal) — garantit la reproductibilité indépendamment de l'environnement Python local.

```bash
# Construire l'image de tests (une seule fois)
docker compose --profile tests build great-expectations

# Lancer les tests
docker compose --profile tests run --rm great-expectations
```

**5 expectations** valident la table de faits Gold :

| # | Test | But |
|---|---|---|
| 1 | `row_count == 161` | Volumétrie attendue (détecte doublons/pertes) |
| 2 | `total_cost` ∈ [0, 10 000] | Garde-fou contre le bug de surcoût |
| 3 | `gross_salary` ∈ [20 000, 100 000] | Cohérence des salaires |
| 4 | `business_unit` ∈ {Finance, Support, Ventes, Marketing, R&D} | Référentiel valide |
| 5 | `employee_id` non nul | Intégrité de la clé |

Résultat attendu : **5/5**.

---

## Interfaces web

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8088 | admin / cf `.env` |
| Redpanda Console | http://localhost:8080 | — |
| Spark Master | http://localhost:8081 | — |
| Kafka Connect (API) | http://localhost:8083 | — |
| MinIO Console | http://localhost:9001 | cf `.env` |
| Grafana | http://localhost:3000 | admin / cf `.env` |
| Prometheus | http://localhost:9090 | — |

---

## Monitoring

Grafana expose un dashboard **Sport Data** avec trois familles de métriques :

- **Débit Kafka** — flux `produce` / `consume` sur le topic `cdc.ops.activities` (`rate(redpanda_kafka_request_bytes_total[5m])`). Détecte un consumer arrêté ou une accumulation.
- **Consumers actifs** — `redpanda_kafka_consumer_group_consumers`. Une chute à 0 signale un streaming interrompu.
- **Volumétrie lakehouse** — `minio_cluster_usage_total_bytes` et `minio_cluster_usage_object_total`. Croissance par paliers à chaque run du pipeline.

> Le lag consumer n'étant pas exposé nativement par Redpanda, le monitoring s'appuie sur le **débit** (métrique de santé équivalente, réellement disponible).

> Les sources de données (Prometheus, PostgreSQL) et les dashboards Grafana sont configurés manuellement via l'interface (http://localhost:3000, admin / cf `.env`). Prometheus, lui, est provisionné par `monitoring/prometheus.yml`.

---

## Structure du projet

```
sport-data-poc/
├── docker-compose.yml          ← orchestration de toute la stack (11 services)
├── .env.example                ← variables à recopier en .env
├── docker/
│   ├── spark/                  ← image Spark + JARs Delta/Kafka/S3
│   ├── simulator/              ← simulateur Strava (génère des activités en continu)
│   └── great-expectations/     ← image des tests de qualité
├── sql/init/                   ← DDL Postgres (auto-exécuté au 1er démarrage)
├── connectors/
│   └── postgres-source.json    ← config du connecteur Debezium
├── jobs/
│   ├── batch/
│   │   ├── bronze.py           ← ingestion brute Postgres → Delta
│   │   ├── silver.py           ← nettoyage + géocodage + distances
│   │   └── gold.py             ← calcul des éligibilités → Delta + Postgres
│   └── streaming/
│       └── notifier.py         ← consommation Redpanda → notification Slack
├── airflow/dags/
│   └── sport_data_pipeline.py  ← DAG médaillon (bronze >> silver >> gold)
├── tests/
│   └── run_quality_checks.py   ← suite Great Expectations (5 expectations)
└── monitoring/
    └── prometheus.yml          ← cibles de scraping (Redpanda, MinIO, Prometheus)
```

---

## Paramétrage métier

Les règles de calcul sont **externalisées** dans la table `analytics.dim_parameters` (modifiables sans toucher au code) :

| Paramètre | Valeur par défaut | Description |
|---|---|---|
| `premium_rate` | 0.0500 | Taux de la prime mobilité (5 %) |
| `wellbeing_threshold` | 15 | Nombre d'activités requis pour le bien-être |
| `wellbeing_days` | 5 | Jours de congé bien-être accordés |
| `max_distance_walk_km` | 15 | Distance max éligible à pied |
| `max_distance_bike_km` | 25 | Distance max éligible à vélo |

Le job Gold lit la ligne la plus récente (`ORDER BY snapshot_date DESC LIMIT 1`). Modifier un paramètre puis relancer le DAG suffit à recalculer les éligibilités.

---

## Dépannage

**Le connecteur Debezium renvoie une erreur 409 (Conflict)**
Le connecteur existe déjà. Le supprimer puis le recréer :
```bash
curl.exe -X DELETE http://localhost:8083/connectors/postgres-cdc-source
```

**Power BI ne se connecte pas à Postgres**
Vérifier que vous ciblez bien le port **5433** (côté hôte), pas 5432 — une instance Postgres locale peut occuper le 5432 et intercepter la connexion.

**Le webserver Airflow met du temps à démarrer / timeout**
Le timeout Gunicorn est porté à 300 s dans le compose. Laisser 2-3 min au premier démarrage.

**Le DAG échoue avec une erreur de version Python**
Le driver (Airflow) et le worker Spark doivent être alignés sur Python 3.8. C'est déjà configuré dans les images fournies.

**Re-run du pipeline : idempotence**
Le pipeline est idempotent. Delta utilise `replaceWhere` par `snapshot_date` ; l'export Postgres utilise `overwrite` + `truncate=true`. Un re-run produit exactement le même résultat, sans doublon.

---

## Choix techniques (justification)

- **Redpanda plutôt que Kafka** — compatible API Kafka, sans Zookeeper, plus léger à conteneuriser pour un POC.
- **Delta Lake** — transactions ACID sur le lakehouse, time travel, `replaceWhere` pour l'idempotence.
- **Séparation Delta / Postgres** — le lakehouse porte l'historique, Postgres ne sert que la BI. Évite la redondance et clarifie les responsabilités.
- **CDC plutôt que polling** — réaction à l'événement, pas d'interrogation périodique de la base.
- **Tests conteneurisés** — reproductibilité, cohérence avec l'architecture tout-Docker.
- **Monitoring du débit** — métrique de santé réellement exposée par Redpanda, à défaut du lag consumer.

---

## Pistes d'industrialisation

- Migrer l'API Google Distance Matrix vers Routes API (plus précise).
- Externaliser les secrets (Vault) plutôt que `.env`.
- Séparer l'export Postgres dans un job dédié.
- Provisionner les dashboards Grafana en JSON (reproductibilité au démarrage).
- CI/CD et déploiement sur cluster managé (Kubernetes).
