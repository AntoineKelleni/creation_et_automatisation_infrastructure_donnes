import os
from sqlalchemy import create_engine, text


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def pg_conn_string() -> str:
    host = env("PG_HOST")
    port = env("PG_PORT", "5432")
    db = env("PG_DB")
    user = env("PG_USER")
    pwd = env("PG_PASSWORD")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def scalar(conn, sql: str, params: dict | None = None):
    return conn.execute(text(sql), params or {}).scalar()


def check(name: str, ok: bool, details: str = "") -> bool:
    status = "OK" if ok else "FAIL"
    print(f"- {name}: {status}" + (f" | {details}" if details else ""))
    return ok


def main() -> int:
    # Volumétrie "attendue" après génération des activités (sera SKIP si table vide)
    min_activities_rows = int(os.getenv("MIN_ACTIVITIES_ROWS", "2000"))

    engine = create_engine(pg_conn_string(), pool_pre_ping=True)
    all_ok = True

    with engine.connect() as conn:
        # ============================================================
        # 1) CLEAN.EMPLOYEES — cohérence (non-null, unique, bornes)
        # ============================================================
        emp_cnt = scalar(conn, "SELECT COUNT(*) FROM clean.employees")
        all_ok &= check("clean.employees row_count > 0", emp_cnt > 0, f"row_count={emp_cnt}")

        # employee_id NOT NULL
        emp_null_id = scalar(conn, "SELECT COUNT(*) FROM clean.employees WHERE employee_id IS NULL")
        all_ok &= check("clean.employees employee_id NOT NULL", emp_null_id == 0, f"nulls={emp_null_id}")

        # employee_id UNIQUE
        emp_dup_id = scalar(conn, """
            SELECT COUNT(*) FROM (
              SELECT employee_id
              FROM clean.employees
              GROUP BY employee_id
              HAVING COUNT(*) > 1
            ) t
        """)
        all_ok &= check("clean.employees employee_id UNIQUE", emp_dup_id == 0, f"duplicates={emp_dup_id}")

        # gross_salary NOT NULL + > 0
        sal_null = scalar(conn, "SELECT COUNT(*) FROM clean.employees WHERE gross_salary IS NULL")
        all_ok &= check("clean.employees gross_salary NOT NULL", sal_null == 0, f"nulls={sal_null}")

        sal_bad = scalar(conn, "SELECT COUNT(*) FROM clean.employees WHERE gross_salary <= 0")
        all_ok &= check("clean.employees gross_salary > 0", sal_bad == 0, f"bad={sal_bad}")

        # home_address NOT NULL/empty
        addr_null = scalar(conn, """
            SELECT COUNT(*)
            FROM clean.employees
            WHERE home_address IS NULL OR home_address = ''
        """)
        all_ok &= check("clean.employees home_address NOT NULL", addr_null == 0, f"null_or_empty={addr_null}")

        # commute_mode NOT NULL/empty
        mode_null = scalar(conn, """
            SELECT COUNT(*)
            FROM clean.employees
            WHERE commute_mode IS NULL OR commute_mode = ''
        """)
        all_ok &= check("clean.employees commute_mode NOT NULL", mode_null == 0, f"null_or_empty={mode_null}")

        print("")

        # ============================================================
        # 2) CLEAN.ACTIVITIES — cohérence (non négatif, dates valides)
        #     (peut être vide tant que pas généré)
        # ============================================================
        try:
            act_cnt = scalar(conn, "SELECT COUNT(*) FROM clean.activities")
            print(f"clean.activities row_count = {act_cnt}")

            if act_cnt == 0:
                all_ok &= check("clean.activities tests (SKIP)", True, "table empty (not generated yet)")
            else:
                # activity_id NOT NULL + unique
                act_null_id = scalar(conn, "SELECT COUNT(*) FROM clean.activities WHERE activity_id IS NULL")
                all_ok &= check("clean.activities activity_id NOT NULL", act_null_id == 0, f"nulls={act_null_id}")

                act_dup_id = scalar(conn, """
                    SELECT COUNT(*) FROM (
                      SELECT activity_id
                      FROM clean.activities
                      GROUP BY activity_id
                      HAVING COUNT(*) > 1
                    ) t
                """)
                all_ok &= check("clean.activities activity_id UNIQUE", act_dup_id == 0, f"duplicates={act_dup_id}")

                # employee_id NOT NULL
                act_emp_null = scalar(conn, "SELECT COUNT(*) FROM clean.activities WHERE employee_id IS NULL")
                all_ok &= check("clean.activities employee_id NOT NULL", act_emp_null == 0, f"nulls={act_emp_null}")

                # elapsed_s >= 0
                elapsed_bad = scalar(conn, "SELECT COUNT(*) FROM clean.activities WHERE elapsed_s < 0")
                all_ok &= check("clean.activities elapsed_s >= 0", elapsed_bad == 0, f"bad={elapsed_bad}")

                # distance_m >= 0 when not null
                dist_bad = scalar(conn, """
                    SELECT COUNT(*)
                    FROM clean.activities
                    WHERE distance_m IS NOT NULL AND distance_m < 0
                """)
                all_ok &= check("clean.activities distance_m >= 0 (when present)", dist_bad == 0, f"bad={dist_bad}")

                # dates non nulles
                start_null = scalar(conn, "SELECT COUNT(*) FROM clean.activities WHERE start_ts IS NULL")
                end_null = scalar(conn, "SELECT COUNT(*) FROM clean.activities WHERE end_ts IS NULL")
                all_ok &= check("clean.activities start_ts NOT NULL", start_null == 0, f"nulls={start_null}")
                all_ok &= check("clean.activities end_ts NOT NULL", end_null == 0, f"nulls={end_null}")

                # end_ts >= start_ts
                date_bad = scalar(conn, """
                    SELECT COUNT(*)
                    FROM clean.activities
                    WHERE start_ts IS NOT NULL
                      AND end_ts IS NOT NULL
                      AND end_ts < start_ts
                """)
                all_ok &= check("clean.activities end_ts >= start_ts", date_bad == 0, f"bad={date_bad}")

                # volumétrie (à appliquer une fois les activités générées)
                if act_cnt < min_activities_rows:
                    all_ok &= check(
                        "volumetry.activities_min_rows (post-generation)",
                        False,
                        f"row_count={act_cnt} < min_required={min_activities_rows}"
                    )
                else:
                    all_ok &= check(
                        "volumetry.activities_min_rows (post-generation)",
                        True,
                        f"row_count={act_cnt} >= min_required={min_activities_rows}"
                    )
        except Exception:
            all_ok &= check("clean.activities tests (SKIP)", True, "table missing (not generated yet)")

        print("")

        # ============================================================
        # 3) RAW.COMMUTE_DISTANCE_CHECKS — cohérence + règle métier 15/25 km
        #     (peut être vide tant que pas calculé/mock)
        # ============================================================
        try:
            cc_cnt = scalar(conn, "SELECT COUNT(*) FROM raw.commute_distance_checks")
            print(f"raw.commute_distance_checks row_count = {cc_cnt}")

            if cc_cnt == 0:
                all_ok &= check("raw.commute_distance_checks tests (SKIP)", True, "table empty (API/mock not run yet)")
                all_ok &= check(
                    "business_rule.max_commute_distance_by_mode (SKIP)",
                    True,
                    "commute_distance_checks empty"
                )
            else:
                # employee_id NOT NULL
                cc_null_emp = scalar(conn, """
                    SELECT COUNT(*)
                    FROM raw.commute_distance_checks
                    WHERE employee_id IS NULL
                """)
                all_ok &= check("raw.commute_distance_checks employee_id NOT NULL", cc_null_emp == 0, f"nulls={cc_null_emp}")

                # distance_km >= 0 when present
                cc_dist_bad = scalar(conn, """
                    SELECT COUNT(*)
                    FROM raw.commute_distance_checks
                    WHERE distance_km IS NOT NULL AND distance_km < 0
                """)
                all_ok &= check("raw.commute_distance_checks distance_km >= 0 (when present)", cc_dist_bad == 0, f"bad={cc_dist_bad}")

                # ------------------------------------------------------------
                # RÈGLE MÉTIER : max distance par mode de transport
                # Marche/Running => max 15 km
                # Vélo/Trottinette/Autres => max 25 km
                # ------------------------------------------------------------
                anomalies = scalar(conn, """
                    SELECT COUNT(*)
                    FROM raw.commute_distance_checks c
                    JOIN clean.employees e ON e.employee_id = c.employee_id
                    WHERE c.distance_km IS NOT NULL
                      AND (
                            (lower(e.commute_mode) IN ('marche', 'walk', 'running', 'course', 'course a pied') AND c.distance_km > 15)
                         OR (lower(e.commute_mode) NOT IN ('marche', 'walk', 'running', 'course', 'course a pied') AND c.distance_km > 25)
                      )
                """)

                all_ok &= check(
                    "business_rule.max_commute_distance_by_mode",
                    anomalies == 0,
                    f"anomalies={anomalies} (walk/run<=15km; bike/scooter/other<=25km)"
                )

                # Exemples (top 5) si anomalies
                if anomalies and anomalies > 0:
                    rows = conn.execute(text("""
                        SELECT c.employee_id, e.commute_mode, c.distance_km
                        FROM raw.commute_distance_checks c
                        JOIN clean.employees e ON e.employee_id = c.employee_id
                        WHERE c.distance_km IS NOT NULL
                          AND (
                                (lower(e.commute_mode) IN ('marche', 'walk', 'running', 'course', 'course a pied') AND c.distance_km > 15)
                             OR (lower(e.commute_mode) NOT IN ('marche', 'walk', 'running', 'course', 'course a pied') AND c.distance_km > 25)
                          )
                        ORDER BY c.distance_km DESC
                        LIMIT 5
                    """)).fetchall()

                    print("  examples (top 5):")
                    for r in rows:
                        print(f"   - employee_id={r[0]} mode={r[1]} distance_km={r[2]}")

        except Exception:
            all_ok &= check("raw.commute_distance_checks tests (SKIP)", True, "table missing (API/mock not run yet)")
            all_ok &= check("business_rule.max_commute_distance_by_mode (SKIP)", True, "table missing")

    print("")
    if all_ok:
        print("!!!OK!!! Quality checks passed.")
        return 0

    print("!!!NON!!! Quality checks failed.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
