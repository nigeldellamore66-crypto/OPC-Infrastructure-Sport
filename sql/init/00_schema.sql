-- Init script Postgres — POC Avantages Sportifs
-- Schéma aligné sur les fichiers fournis: Donnees_RH.xlsx et Donnees_Sportives.xlsx


-- 1. Base séparée pour Airflow
CREATE DATABASE airflow;

\c sportdata;

-- 2. Schémas applicatifs
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS analytics;

-- 3. Table RH — colonnes calquées sur Donnees_RH.xlsx
CREATE TABLE hr.employees (
    employee_id           INTEGER PRIMARY KEY,                -- ID salarié
    last_name             VARCHAR(100) NOT NULL,              -- Nom
    first_name            VARCHAR(100) NOT NULL,              -- Prénom
    birth_date            DATE NOT NULL,                      -- Date de naissance
    business_unit         VARCHAR(50) NOT NULL,               -- BU
    hire_date             DATE NOT NULL,                      -- Date d'embauche
    gross_salary          NUMERIC(10,2) NOT NULL,             -- Salaire brut
    contract_type         VARCHAR(10) NOT NULL,               -- Type de contrat
    paid_leave_days       INTEGER NOT NULL,                   -- Nombre de jours de CP
    home_address          VARCHAR(255) NOT NULL,              -- Adresse du domicile (libre)
    transport_mode        VARCHAR(50) NOT NULL,               -- Moyen de déplacement
    -- Méta techniques
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_contract_type CHECK (contract_type IN ('CDI', 'CDD')),
    CONSTRAINT chk_transport_mode CHECK (transport_mode IN (
        'Marche/running',
        'Vélo/Trottinette/Autres',
        'véhicule thermique/électrique',
        'Transports en commun'
    ))
);

CREATE INDEX idx_employees_transport ON hr.employees(transport_mode);
CREATE INDEX idx_employees_bu ON hr.employees(business_unit);

-- Trigger updated_at automatique
CREATE OR REPLACE FUNCTION hr.set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_employees_updated_at
    BEFORE UPDATE ON hr.employees
    FOR EACH ROW EXECUTE FUNCTION hr.set_updated_at();


-- 4. Table Activités — alimentée par le simulateur (historique + live)

CREATE TABLE ops.activities (
    activity_id      BIGSERIAL PRIMARY KEY,
    employee_id      INTEGER NOT NULL REFERENCES hr.employees(employee_id),
    sport_type       VARCHAR(50) NOT NULL,                   -- 'Course à pied', 'Vélo', 'Randonnée', etc.
    start_at         TIMESTAMPTZ NOT NULL,                    -- Date de début
    end_at           TIMESTAMPTZ NOT NULL,                    -- Date de fin (déduite)
    distance_m       INTEGER,                                 -- Nullable pour escalade, yoga, etc.
    duration_s       INTEGER NOT NULL,                        -- Temps écoulé
    comment          TEXT,                                    -- Commentaire libre
    source           VARCHAR(20) NOT NULL DEFAULT 'live',     -- 'historical' ou 'live'
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_distance_positive CHECK (distance_m IS NULL OR distance_m >= 0),
    CONSTRAINT chk_duration_positive CHECK (duration_s > 0),
    CONSTRAINT chk_dates_coherent CHECK (end_at > start_at),
    CONSTRAINT chk_source_valid CHECK (source IN ('historical', 'live'))
);

CREATE INDEX idx_activities_employee ON ops.activities(employee_id);
CREATE INDEX idx_activities_start ON ops.activities(start_at);
CREATE INDEX idx_activities_source ON ops.activities(source);
CREATE INDEX idx_activities_sport ON ops.activities(sport_type);

-- 5. Tables analytics — alimentées par le job Spark Gold → Postgres

CREATE TABLE analytics.fct_employee_eligibility (
    snapshot_date              DATE NOT NULL,
    employee_id                INTEGER NOT NULL,
    full_name                  VARCHAR(200),
    business_unit              VARCHAR(50),
    contract_type              VARCHAR(10),
    gross_salary               NUMERIC(10,2),
    transport_mode             VARCHAR(50),
    home_to_office_km          NUMERIC(8,2),
    distance_check_status      VARCHAR(20),
    is_eligible_premium        BOOLEAN NOT NULL,
    premium_amount             NUMERIC(10,2) NOT NULL DEFAULT 0,
    activities_count_12m       INTEGER NOT NULL DEFAULT 0,
    is_eligible_wellbeing      BOOLEAN NOT NULL,
    wellbeing_days             INTEGER NOT NULL DEFAULT 0,
    wellbeing_cost             NUMERIC(10,2) NOT NULL DEFAULT 0,   
    total_cost                 NUMERIC(10,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (snapshot_date, employee_id)
);

CREATE TABLE analytics.fct_activities_enriched (
    activity_id      BIGINT PRIMARY KEY,
    employee_id      INTEGER NOT NULL,
    full_name        VARCHAR(200),
    business_unit    VARCHAR(50),
    sport_type       VARCHAR(50),
    start_at         TIMESTAMPTZ,
    distance_km      NUMERIC(8,2),
    duration_min     NUMERIC(8,2),
    pace_min_per_km  NUMERIC(6,2),
    comment          TEXT
);

CREATE TABLE analytics.dim_parameters (
    snapshot_date          DATE PRIMARY KEY,
    premium_rate           NUMERIC(5,4) NOT NULL,            -- 0.05 par défaut
    wellbeing_threshold    INTEGER NOT NULL,                 -- 15 activités/an
    wellbeing_days         INTEGER NOT NULL,                 -- 5 jours
    max_distance_walk_km   INTEGER NOT NULL,                 -- 15 km
    max_distance_bike_km   INTEGER NOT NULL                  -- 25 km
);

INSERT INTO analytics.dim_parameters VALUES
    (CURRENT_DATE, 0.05, 15, 5, 15, 25);

-- 6. Configuration Debezium pour CDC
-- User dédié avec droits de lecture seule + réplication
CREATE USER debezium WITH REPLICATION ENCRYPTED PASSWORD 'debezium_replication_pwd';
GRANT CONNECT ON DATABASE sportdata TO debezium;
GRANT USAGE ON SCHEMA hr, ops TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA hr, ops TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA hr, ops GRANT SELECT ON TABLES TO debezium;

-- Publication des tables à tracker
CREATE PUBLICATION debezium_pub
    FOR TABLE hr.employees,  ops.activities;

-- REPLICA IDENTITY FULL pour avoir les anciennes valeurs en UPDATE/DELETE
ALTER TABLE hr.employees        REPLICA IDENTITY FULL;
ALTER TABLE ops.activities      REPLICA IDENTITY FULL;