import os
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
    f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}"
)

rh = pd.read_excel("/opt/airflow/project/data/input/Données RH.xlsx")
sp = pd.read_excel("/opt/airflow/project/data/input/Données Sportive.xlsx")

rh_raw = pd.DataFrame({
    "source_file": "Données RH.xlsx",
    "employee_id_raw": rh["ID salarié"],
    "last_name_raw": rh["Nom"],
    "first_name_raw": rh["Prénom"],
    "birth_date_raw": rh["Date de naissance"],
    "bu_raw": rh["BU"],
    "hire_date_raw": rh["Date d'embauche"],
    "gross_salary_raw": rh["Salaire brut"],
    "contract_type_raw": rh["Type de contrat"],
    "paid_leave_raw": rh["Nombre de jours de CP"],
    "home_address_raw": rh["Adresse du domicile"],
    "commute_mode_raw": rh["Moyen de déplacement"],
})

sp_raw = pd.DataFrame({
    "source_file": "Données Sportive.xlsx",
    "employee_id_raw": sp["ID salarié"],
    "sport_practice_raw": sp["Pratique d'un sport"],
})

rh_raw.to_sql("rh_employees_snapshot", engine, schema="raw", if_exists="append", index=False)
sp_raw.to_sql("sport_profile_snapshot", engine, schema="raw", if_exists="append", index=False)

print("RAW ingestion OK")
