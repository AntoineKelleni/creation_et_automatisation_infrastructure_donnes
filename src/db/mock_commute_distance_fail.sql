-- Mock "FAIL" : injecte 15 lignes dont certaines incohérentes (50 km)
-- Objectif : faire échouer la règle métier max distance par mode

TRUNCATE TABLE raw.commute_distance_checks;

INSERT INTO raw.commute_distance_checks (
    employee_id,
    home_address,
    company_address,
    commute_mode,
    distance_km,
    duration_s,
    api_provider,
    api_status,
    is_within_rule,
    rule_name,
    anomaly_reason
)
SELECT
    e.employee_id,
    e.home_address,
    '1362 Av. des Platanes, 34970 Lattes' AS company_address,
    e.commute_mode,
    CASE
        WHEN e.employee_id % 3 = 0 THEN 50.000   -- volontairement incohérent
        WHEN e.employee_id % 3 = 1 THEN 10.000   -- cohérent
        ELSE 20.000                              -- cohérent
    END AS distance_km,
    3600 AS duration_s,
    'MOCK' AS api_provider,
    'OK' AS api_status,
    NULL AS is_within_rule,
    'MAX_COMMUTE_DISTANCE_BY_MODE' AS rule_name,
    CASE
        WHEN e.employee_id % 3 = 0 THEN 'Distance incohérente par rapport au mode de transport'
        ELSE NULL
    END AS anomaly_reason
FROM clean.employees e
LIMIT 15;
