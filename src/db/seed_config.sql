INSERT INTO meta.config_parameters (param_name, param_value_text, param_type, unit, description)
VALUES
('prime_rate', '0.05', 'float', 'ratio', 'Prime sportive'),
('min_activities_year', '15', 'int', 'count', 'Min activités/an'),
('max_wellbeing_days', '5', 'int', 'days', 'Plafond jours bien-être'),
('max_distance_walk_run_km', '15', 'int', 'km', 'Max marche/running'),
('max_distance_bike_scooter_km', '25', 'int', 'km', 'Max vélo/trottinette'),
('company_address', '1362 Av. des Platanes, 34970 Lattes', 'text', NULL, 'Adresse entreprise');
