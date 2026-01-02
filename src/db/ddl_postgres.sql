-- ============================================================
-- POC Avantages Sportifs - PostgreSQL DDL
-- Schémas : raw / clean / analytics / meta
-- ============================================================

-- 0) Schémas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS meta;

-- 1) Paramètres (règles "non fixes" -> rejouabilité)
-- La note précise que les paramètres peuvent évoluer. :contentReference[oaicite:1]{index=1}
CREATE TABLE IF NOT EXISTS meta.config_parameters (
  param_name        TEXT PRIMARY KEY,
  param_value_text  TEXT NOT NULL,
  param_type        TEXT NOT NULL CHECK (param_type IN ('int','float','bool','text')),
  unit              TEXT NULL,
  description       TEXT NULL,
  effective_from    TIMESTAMPTZ NOT NULL DEFAULT now(),
  effective_to      TIMESTAMPTZ NULL
);

-- 2) Monitoring / Runs
CREATE TABLE IF NOT EXISTS meta.pipeline_run (
  run_id            BIGSERIAL PRIMARY KEY,
  dag_id            TEXT NOT NULL,
  execution_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
  status            TEXT NOT NULL CHECK (status IN ('running','success','failed')),
  triggered_by      TEXT NULL,
  notes             TEXT NULL
);

CREATE TABLE IF NOT EXISTS meta.pipeline_task_run (
  task_run_id       BIGSERIAL PRIMARY KEY,
  run_id            BIGINT NOT NULL REFERENCES meta.pipeline_run(run_id) ON DELETE CASCADE,
  task_id           TEXT NOT NULL,
  started_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_ts          TIMESTAMPTZ NULL,
  status            TEXT NOT NULL CHECK (status IN ('running','success','failed','skipped')),
  rows_in           BIGINT NULL,
  rows_out          BIGINT NULL,
  error_message     TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_run_run_id ON meta.pipeline_task_run(run_id);

-- ============================================================
-- RAW ZONE (ingestion brute, historisée)
-- ============================================================

-- Snapshot RH (on garde les colonnes en texte pour ne pas casser l’ingestion)
CREATE TABLE IF NOT EXISTS raw.rh_employees_snapshot (
  snapshot_id       BIGSERIAL PRIMARY KEY,
  ingested_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_file       TEXT NOT NULL,

  employee_id_raw   TEXT NULL,
  last_name_raw     TEXT NULL,
  first_name_raw    TEXT NULL,
  birth_date_raw    TEXT NULL,
  bu_raw            TEXT NULL,
  hire_date_raw     TEXT NULL,
  gross_salary_raw  TEXT NULL,
  contract_type_raw TEXT NULL,
  paid_leave_raw    TEXT NULL,
  home_address_raw  TEXT NULL,
  commute_mode_raw  TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_rh_ingested_ts ON raw.rh_employees_snapshot(ingested_ts);
CREATE INDEX IF NOT EXISTS idx_raw_rh_employee_id_raw ON raw.rh_employees_snapshot(employee_id_raw);

-- Snapshot "profil sport" (fichier Données Sportive.xlsx)
CREATE TABLE IF NOT EXISTS raw.sport_profile_snapshot (
  snapshot_id       BIGSERIAL PRIMARY KEY,
  ingested_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_file       TEXT NOT NULL,

  employee_id_raw   TEXT NULL,
  sport_practice_raw TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_sport_ingested_ts ON raw.sport_profile_snapshot(ingested_ts);
CREATE INDEX IF NOT EXISTS idx_raw_sport_employee_id_raw ON raw.sport_profile_snapshot(employee_id_raw);

-- Données Strava-like simulées (12 mois, milliers de lignes) :contentReference[oaicite:2]{index=2}
CREATE TABLE IF NOT EXISTS raw.activities_simulated (
  activity_id       BIGSERIAL PRIMARY KEY,
  generated_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
  employee_id       BIGINT NOT NULL,
  start_ts          TIMESTAMPTZ NOT NULL,
  sport_type        TEXT NOT NULL,
  distance_m        INTEGER NULL,          -- nullable si non pertinent :contentReference[oaicite:3]{index=3}
  end_ts            TIMESTAMPTZ NOT NULL,
  elapsed_s         INTEGER NOT NULL,
  comment_text      TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_act_employee_start ON raw.activities_simulated(employee_id, start_ts);

-- Résultats contrôle distance domicile -> entreprise via Google Maps :contentReference[oaicite:4]{index=4}
CREATE TABLE IF NOT EXISTS raw.commute_distance_checks (
  check_id          BIGSERIAL PRIMARY KEY,
  computed_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
  employee_id       BIGINT NOT NULL,
  home_address      TEXT NOT NULL,
  company_address   TEXT NOT NULL,
  commute_mode      TEXT NOT NULL,

  distance_km       NUMERIC(8,3) NULL,
  duration_s        INTEGER NULL,
  api_provider      TEXT NOT NULL,          -- 'google_maps' ou 'mock'
  api_status        TEXT NOT NULL,          -- 'OK','ERROR',...
  is_within_rule    BOOLEAN NULL,
  rule_name         TEXT NULL,
  anomaly_reason    TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_commute_employee_ts ON raw.commute_distance_checks(employee_id, computed_ts);

-- ============================================================
-- CLEAN ZONE (données typées, normalisées)
-- ============================================================

CREATE TABLE IF NOT EXISTS clean.employees (
  employee_id       BIGINT PRIMARY KEY,
  last_name         TEXT NOT NULL,
  first_name        TEXT NOT NULL,
  birth_date        DATE NULL,
  bu               TEXT NULL,
  hire_date         DATE NULL,
  gross_salary      NUMERIC(12,2) NOT NULL,
  contract_type     TEXT NULL,
  paid_leave_days   INTEGER NULL,
  home_address      TEXT NULL,
  commute_mode      TEXT NULL,

  sport_profile     TEXT NULL,
  is_active         BOOLEAN NOT NULL DEFAULT true,
  updated_ts        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_clean_employees_bu ON clean.employees(bu);

CREATE TABLE IF NOT EXISTS clean.activities (
  activity_id       BIGINT PRIMARY KEY,     -- reprend raw.activities_simulated.activity_id
  employee_id       BIGINT NOT NULL REFERENCES clean.employees(employee_id),
  start_ts          TIMESTAMPTZ NOT NULL,
  end_ts            TIMESTAMPTZ NOT NULL,
  sport_type        TEXT NOT NULL,
  distance_m        INTEGER NULL CHECK (distance_m IS NULL OR distance_m >= 0),
  elapsed_s         INTEGER NOT NULL CHECK (elapsed_s >= 0),
  comment_text      TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_clean_activities_employee_start ON clean.activities(employee_id, start_ts);

CREATE TABLE IF NOT EXISTS clean.commute_validation (
  employee_id       BIGINT PRIMARY KEY REFERENCES clean.employees(employee_id),
  last_computed_ts  TIMESTAMPTZ NOT NULL,
  distance_km       NUMERIC(8,3) NULL,
  commute_mode      TEXT NULL,
  is_within_rule    BOOLEAN NULL,
  rule_name         TEXT NULL,
  anomaly_reason    TEXT NULL,
  api_provider      TEXT NOT NULL,
  api_status        TEXT NOT NULL
);

-- ============================================================
-- ANALYTICS ZONE (prêt Power BI)
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.dim_employee (
  employee_id       BIGINT PRIMARY KEY,
  last_name         TEXT NOT NULL,
  first_name        TEXT NOT NULL,
  bu               TEXT NULL,
  hire_date         DATE NULL,
  gross_salary      NUMERIC(12,2) NOT NULL,
  contract_type     TEXT NULL,
  sport_profile     TEXT NULL,
  commute_mode      TEXT NULL
);

CREATE TABLE IF NOT EXISTS analytics.fact_activity (
  activity_id       BIGINT PRIMARY KEY,
  employee_id       BIGINT NOT NULL REFERENCES analytics.dim_employee(employee_id),
  activity_date     DATE NOT NULL,
  sport_type        TEXT NOT NULL,
  distance_m        INTEGER NULL,
  elapsed_s         INTEGER NOT NULL,
  comment_text      TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_activity_employee_date ON analytics.fact_activity(employee_id, activity_date);

-- Prime sportive : 5% salaire annuel brut (paramétrable) :contentReference[oaicite:5]{index=5}
CREATE TABLE IF NOT EXISTS analytics.fact_prime_sportive (
  employee_id       BIGINT NOT NULL REFERENCES analytics.dim_employee(employee_id),
  year              INTEGER NOT NULL,
  gross_salary      NUMERIC(12,2) NOT NULL,
  prime_rate        NUMERIC(6,4) NOT NULL,
  prime_amount      NUMERIC(12,2) NOT NULL,
  is_eligible       BOOLEAN NOT NULL,
  eligibility_reason TEXT NULL,
  computed_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (employee_id, year)
);

-- Jours bien-être : >= 15 activités/an, plafonné à 5 :contentReference[oaicite:6]{index=6}
CREATE TABLE IF NOT EXISTS analytics.fact_jours_bien_etre (
  employee_id       BIGINT NOT NULL REFERENCES analytics.dim_employee(employee_id),
  year              INTEGER NOT NULL,
  activities_count  INTEGER NOT NULL,
  min_required      INTEGER NOT NULL,
  max_days          INTEGER NOT NULL,
  days_granted      INTEGER NOT NULL,
  is_eligible       BOOLEAN NOT NULL,
  computed_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (employee_id, year)
);

-- Anomalies de déclaration (distance incohérente selon règles 15/25 km) :contentReference[oaicite:7]{index=7}
CREATE TABLE IF NOT EXISTS analytics.fact_anomalies_declaration (
  anomaly_id        BIGSERIAL PRIMARY KEY,
  employee_id       BIGINT NOT NULL REFERENCES analytics.dim_employee(employee_id),
  computed_ts       TIMESTAMPTZ NOT NULL,
  commute_mode      TEXT NOT NULL,
  distance_km       NUMERIC(8,3) NULL,
  rule_name         TEXT NULL,
  anomaly_reason    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anom_employee_ts ON analytics.fact_anomalies_declaration(employee_id, computed_ts);

-- KPI de monitoring à afficher (Power BI ou page monitoring)
CREATE TABLE IF NOT EXISTS analytics.kpi_pipeline_daily (
  kpi_date          DATE NOT NULL,
  dag_id            TEXT NOT NULL,
  rows_rh_ingested  BIGINT NULL,
  rows_sport_ingested BIGINT NULL,
  rows_activities_generated BIGINT NULL,
  anomalies_count   BIGINT NULL,
  status            TEXT NOT NULL,
  PRIMARY KEY (kpi_date, dag_id)
);
