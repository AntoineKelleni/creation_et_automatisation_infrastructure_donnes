BEGIN;

CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.employee_benefits;

CREATE TABLE analytics.employee_benefits (
  employee_id        bigint PRIMARY KEY,
  eligibility_status text   NOT NULL,
  eligible_commute   boolean NOT NULL,
  eligible_wellness  boolean NOT NULL,

  -- Avantages
  sport_prime_eur    numeric(12,2) NOT NULL,
  wellness_days      integer       NOT NULL,

  -- Détails de contrôle (debug / justification)
  commute_mode       text,
  sport_profile      text,
  gross_salary       numeric(12,2),
  nb_activities_year integer NOT NULL,
  nb_sport_activities_year integer NOT NULL,

  -- Règles appliquées (trace)
  rule_prime         text NOT NULL,
  rule_wellness      text NOT NULL,
  computed_ts        timestamptz NOT NULL DEFAULT now()
);

WITH
-- R5/R6 : on compte les activités de l'année, en filtrant les activités "valides"
activities_year AS (
  SELECT
    a.employee_id,
    COUNT(*) FILTER (
      WHERE a.start_ts >= date_trunc('year', now())
        AND a.start_ts <  date_trunc('year', now()) + interval '1 year'
    ) AS nb_activities_year,
    COUNT(*) FILTER (
      WHERE a.start_ts >= date_trunc('year', now())
        AND a.start_ts <  date_trunc('year', now()) + interval '1 year'
        AND COALESCE(a.distance_m, 0) > 0
        AND COALESCE(a.elapsed_s, 0) > 0
        AND lower(COALESCE(a.sport_type,'')) IN ('running','walking','cycling','hiking','swimming')
    ) AS nb_sport_activities_year
  FROM clean.activities a
  GROUP BY a.employee_id
),

base AS (
  SELECT
    e.employee_id,
    e.commute_mode,
    e.sport_profile,
    e.gross_salary,
    e.is_active,
    COALESCE(ay.nb_activities_year, 0) AS nb_activities_year,
    COALESCE(ay.nb_sport_activities_year, 0) AS nb_sport_activities_year
  FROM clean.employees e
  LEFT JOIN activities_year ay
    ON ay.employee_id = e.employee_id
),

rules AS (
  SELECT
    b.*,

    -- R1 : trajet "sportif" basé sur le déclaratif RH (commute_mode)
    CASE
      WHEN b.commute_mode IS NULL OR btrim(b.commute_mode) = '' THEN FALSE
      WHEN lower(b.commute_mode) LIKE '%marche%' THEN TRUE
      WHEN lower(b.commute_mode) LIKE '%running%' THEN TRUE
      WHEN lower(b.commute_mode) LIKE '%course%' THEN TRUE
      WHEN lower(b.commute_mode) LIKE '%vélo%'  THEN TRUE   -- important (accent)
      WHEN lower(b.commute_mode) LIKE '%velo%'  THEN TRUE   -- si jamais sans accent
      WHEN lower(b.commute_mode) LIKE '%trottinette%' THEN TRUE
      ELSE FALSE
    END AS eligible_commute,

    -- R4 : éligibilité "journées bien-être" : au moins 15 activités sportives validées dans l'année
    (b.nb_sport_activities_year >= 15) AS eligible_wellness

  FROM base b
),

computed AS (
  SELECT
    r.*,

    -- R8 : si inactif => non éligible global
    CASE WHEN r.is_active IS TRUE THEN TRUE ELSE FALSE END AS active_ok,

    -- R3 Prime sportive : 5% du salaire annuel brut si éligible + actif (sans plafond)
    CASE
      WHEN r.is_active IS TRUE
       AND r.eligible_commute IS TRUE
       AND COALESCE(r.gross_salary, 0) > 0
      THEN round(r.gross_salary * 0.05, 2)
      ELSE 0
    END AS sport_prime_eur,

    -- R7 : 5 jours bien-être si éligible + actif
    CASE
      WHEN r.is_active IS TRUE AND r.eligible_wellness IS TRUE THEN 5
      ELSE 0
    END AS wellness_days_calc,

    -- R9/R10 : statut global + trace des règles
    CASE
      WHEN r.is_active IS NOT TRUE THEN 'NON_ELIGIBLE'
      WHEN (r.eligible_commute IS TRUE) OR (r.eligible_wellness IS TRUE) THEN 'ELIGIBLE'
      ELSE 'NON_ELIGIBLE'
    END AS eligibility_status,

    'R1+R2+R3: commute_mode sportif => 5% salaire brut annuel, actif requis' AS rule_prime,
    'R4+R5+R6+R7: >=15 activités sportives valides/an => 5 jours, actif requis' AS rule_wellness

  FROM rules r
)

INSERT INTO analytics.employee_benefits (
  employee_id,
  eligibility_status,
  eligible_commute,
  eligible_wellness,
  sport_prime_eur,
  wellness_days,
  commute_mode,
  sport_profile,
  gross_salary,
  nb_activities_year,
  nb_sport_activities_year,
  rule_prime,
  rule_wellness
)
SELECT
  c.employee_id,
  c.eligibility_status,
  c.eligible_commute,
  c.eligible_wellness,
  c.sport_prime_eur AS sport_prime_eur,
  c.wellness_days_calc AS wellness_days,
  c.commute_mode,
  c.sport_profile,
  c.gross_salary,
  c.nb_activities_year,
  c.nb_sport_activities_year,
  c.rule_prime,
  c.rule_wellness
FROM computed c;

COMMIT;
