import os
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
    f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}"
)

rh = pd.read_sql("""
SELECT * FROM raw.rh_employees_snapshot
WHERE ingested_ts = (SELECT max(ingested_ts) FROM raw.rh_employees_snapshot)
""", engine)

sp = pd.read_sql("""
SELECT * FROM raw.sport_profile_snapshot
WHERE ingested_ts = (SELECT max(ingested_ts) FROM raw.sport_profile_snapshot)
""", engine)

rh["employee_id"] = pd.to_numeric(rh["employee_id_raw"], errors="coerce")
rh = rh.dropna(subset=["employee_id"])
rh["employee_id"] = rh["employee_id"].astype(int)

sp["employee_id"] = pd.to_numeric(sp["employee_id_raw"], errors="coerce")
sp = sp.dropna(subset=["employee_id"])
sp["employee_id"] = sp["employee_id"].astype(int)

df = rh.merge(
    sp[["employee_id", "sport_practice_raw"]],
    on="employee_id",
    how="left"
)

out = pd.DataFrame({
    "employee_id": df["employee_id"],
    "last_name": df["last_name_raw"],
    "first_name": df["first_name_raw"],
    "birth_date": pd.to_datetime(df["birth_date_raw"], errors="coerce").dt.date,
    "bu": df["bu_raw"],
    "hire_date": pd.to_datetime(df["hire_date_raw"], errors="coerce").dt.date,
    "gross_salary": pd.to_numeric(df["gross_salary_raw"], errors="coerce").fillna(0),
    "contract_type": df["contract_type_raw"],
    "paid_leave_days": pd.to_numeric(df["paid_leave_raw"], errors="coerce"),
    "home_address": df["home_address_raw"],
    "commute_mode": df["commute_mode_raw"],
    "sport_profile": df["sport_practice_raw"],
    "is_active": True,
})

with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE clean.employees CASCADE"))

out.to_sql("employees", engine, schema="clean", if_exists="append", index=False)

print("CLEAN employees OK")
