import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import create_engine, text

# Connexion DB
engine = create_engine(
    f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
    f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}"
)

SPORT_TYPES = ["running", "walking", "cycling"]
ACTIVITIES_PER_EMPLOYEE = (5, 30)  # min, max
MAX_BIGINT = 9_000_000_000_000_000_000  # < 9.22e18 (limite bigint signed)

def make_activity_id_bigint() -> int:
    # UUID -> int -> borné pour entrer dans un bigint
    return uuid.uuid4().int % MAX_BIGINT

def random_activity_window():
    # Fenêtre aléatoire sur ~1 an
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=random.randint(1, 365), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    duration_s = random.randint(10 * 60, 120 * 60)  # 10 à 120 min
    end = start + timedelta(seconds=duration_s)
    return start, end, duration_s

def random_distance_m(sport: str) -> int:
    # distances réalistes (m)
    if sport == "walking":
        return random.randint(1000, 12000)
    if sport == "running":
        return random.randint(1500, 25000)
    return random.randint(2000, 60000)  # cycling

def main():
    # Récupérer les employés clean
    employees = pd.read_sql("SELECT employee_id FROM clean.employees", engine)
    if employees.empty:
        raise RuntimeError("Aucun employé dans clean.employees. Lance d'abord le POC_01.")

    employee_ids = employees["employee_id"].astype(int).tolist()

    records = []
    for emp_id in employee_ids:
        n = random.randint(ACTIVITIES_PER_EMPLOYEE[0], ACTIVITIES_PER_EMPLOYEE[1])
        for _ in range(n):
            sport = random.choice(SPORT_TYPES)
            start_ts, end_ts, elapsed_s = random_activity_window()
            distance_m = random_distance_m(sport)

            records.append(
                {
                    "activity_id": make_activity_id_bigint(),  # <-- BIGINT OK
                    "employee_id": emp_id,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "sport_type": sport,
                    "distance_m": int(distance_m),
                    "elapsed_s": int(elapsed_s),
                    "comment_text": None,
                }
            )

    df = pd.DataFrame(records)

    # Optionnel : éviter doublons (très rare) si jamais mod Bigint collision
    df = df.drop_duplicates(subset=["activity_id"])

    with engine.begin() as conn:
        # si tu veux régénérer proprement :
        conn.execute(text("TRUNCATE TABLE clean.activities"))

    df.to_sql("activities", engine, schema="clean", if_exists="append", index=False, method="multi")
    print(f"[OK] {len(df)} activities insérées dans clean.activities")

if __name__ == "__main__":
    main()
