from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
FILE = ROOT / "data" / "processed" / "gold" / "inventory_scenarios.parquet"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

passed = 0
failed = 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"[PASS] {name}" + (f" — {detail}" if detail else ""))
    else:
        failed += 1
        print(f"[FAIL] {name}" + (f" — {detail}" if detail else ""))

try:
    path = FILE.as_posix().replace("'", "''")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE inv AS
        SELECT * FROM read_parquet('{path}')
    """)

    print("Checking inventory scenario structure...")

    row = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT id),
            COUNT(DISTINCT lead_time_weeks),
            COUNT(DISTINCT target_service_level),
            COUNT(*) FILTER (
                WHERE id IS NULL
                   OR forecast_week IS NULL
                   OR forecast_weekly_demand IS NULL
                   OR historical_std_weekly_demand IS NULL
                   OR lead_time_demand IS NULL
                   OR safety_stock IS NULL
                   OR reorder_point_units IS NULL
                   OR forecast_weekly_demand < 0
                   OR historical_std_weekly_demand < 0
                   OR lead_time_demand < 0
                   OR safety_stock < 0
                   OR reorder_point_units < 0
            )
        FROM inv
    """).fetchone()

    check("Expected scenario row count", row[0] == 274410,
          f"{row[0]:,} rows")
    check("Expected product-store series", row[1] == 30490,
          f"{row[1]:,} series")
    check("Three lead-time assumptions", row[2] == 3,
          f"{row[2]} values")
    check("Three service-level assumptions", row[3] == 3,
          f"{row[3]} values")
    check("No null or negative inventory values", row[4] == 0,
          f"{row[4]} invalid rows")

    duplicates = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT id, lead_time_weeks, target_service_level
            FROM inv
            GROUP BY id, lead_time_weeks, target_service_level
            HAVING COUNT(*) != 1
        )
    """).fetchone()[0]

    check("Unique product-store-scenario keys", duplicates == 0,
          f"{duplicates} duplicate scenario groups")

    wrong_counts = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT id
            FROM inv
            GROUP BY id
            HAVING COUNT(*) != 9
        )
    """).fetchone()[0]

    check("Exactly nine scenarios per series", wrong_counts == 0,
          f"{wrong_counts} series with incorrect counts")

    print("\nChecking calculation formulas...")

    formula_errors = con.execute("""
        SELECT COUNT(*)
        FROM inv
        WHERE ABS(
            lead_time_demand
            - forecast_weekly_demand * lead_time_weeks
        ) > 0.0001
        OR ABS(
            safety_stock
            - z_score * historical_std_weekly_demand
              * SQRT(CAST(lead_time_weeks AS DOUBLE))
        ) > 0.0001
        OR ABS(
            reorder_point_units
            - CEIL(lead_time_demand + safety_stock)
        ) > 0.0001
    """).fetchone()[0]

    check("Lead-time demand, safety stock and ROP formulas",
          formula_errors == 0, f"{formula_errors} formula mismatches")

    print("\nChecking scenario monotonicity...")

    service_violations = con.execute("""
        SELECT COUNT(*)
        FROM inv a
        JOIN inv b
          ON a.id = b.id
         AND a.lead_time_weeks = b.lead_time_weeks
         AND a.target_service_level < b.target_service_level
        WHERE a.reorder_point_units > b.reorder_point_units
    """).fetchone()[0]

    check("Higher service level does not lower ROP",
          service_violations == 0,
          f"{service_violations} violations")

    lead_violations = con.execute("""
        SELECT COUNT(*)
        FROM inv a
        JOIN inv b
          ON a.id = b.id
         AND a.target_service_level = b.target_service_level
         AND a.lead_time_weeks < b.lead_time_weeks
        WHERE a.reorder_point_units > b.reorder_point_units
    """).fetchone()[0]

    check("Longer lead time does not lower ROP",
          lead_violations == 0,
          f"{lead_violations} violations")

    print("\nRESULT:", f"{passed}/{passed + failed} checks passed")
    if failed:
        raise SystemExit(1)

finally:
    con.close()
