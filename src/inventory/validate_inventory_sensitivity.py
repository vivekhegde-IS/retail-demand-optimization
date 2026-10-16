from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "processed" / "gold"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")

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
    def parquet_path(name):
        return (GOLD / name).as_posix().replace("'", "''")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE inv AS
        SELECT * FROM read_parquet(
            '{parquet_path("inventory_scenarios.parquet")}'
        )
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE summary AS
        SELECT * FROM read_parquet(
            '{parquet_path("inventory_scenario_summary.parquet")}'
        )
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE sensitivity AS
        SELECT * FROM read_parquet(
            '{parquet_path("inventory_sensitivity.parquet")}'
        )
    """)

    print("Checking summary output...")
    summary_stats = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT lead_time_weeks),
            COUNT(DISTINCT target_service_level),
            SUM(product_store_series),
            COUNT(*) FILTER (
                WHERE product_store_series != 30490
                   OR avg_safety_stock < 0
                   OR avg_reorder_point_units < 0
            )
        FROM summary
    """).fetchone()

    check("Nine policy-summary rows", summary_stats[0] == 9,
          f"{summary_stats[0]} rows")
    check("Three lead times and three service levels",
          summary_stats[1] == 3 and summary_stats[2] == 3)
    check("Every summary covers all series",
          summary_stats[3] == 9 * 30490,
          f"{summary_stats[3]:,} summed series counts")
    check("Valid summary metrics", summary_stats[4] == 0,
          f"{summary_stats[4]} invalid rows")

    print("\nChecking sensitivity output...")
    sens_stats = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT id),
            COUNT(*) FILTER (
                WHERE id IS NULL
                   OR baseline_reorder_point IS NULL
                   OR high_policy_reorder_point IS NULL
                   OR additional_reorder_point_units IS NULL
                   OR additional_safety_stock_units IS NULL
                   OR baseline_reorder_point < 0
                   OR high_policy_reorder_point < 0
                   OR additional_reorder_point_units < 0
            )
        FROM sensitivity
    """).fetchone()

    check("One sensitivity row per series",
          sens_stats[0] == 30490 and sens_stats[1] == 30490,
          f"{sens_stats[0]:,} rows; {sens_stats[1]:,} unique series")
    check("No invalid sensitivity values", sens_stats[2] == 0,
          f"{sens_stats[2]} invalid rows")

    duplicates = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT id FROM sensitivity
            GROUP BY id HAVING COUNT(*) != 1
        )
    """).fetchone()[0]
    check("Unique sensitivity keys", duplicates == 0,
          f"{duplicates} duplicate keys")

    print("\nRecomputing policy differences from source scenarios...")
    mismatches = con.execute("""
        WITH expected AS (
            SELECT
                id,
                MAX(reorder_point_units) FILTER (
                    WHERE lead_time_weeks = 1
                      AND target_service_level = 0.90
                ) AS baseline_rop,
                MAX(reorder_point_units) FILTER (
                    WHERE lead_time_weeks = 4
                      AND target_service_level = 0.99
                ) AS high_rop,
                MAX(safety_stock) FILTER (
                    WHERE lead_time_weeks = 1
                      AND target_service_level = 0.90
                ) AS baseline_ss,
                MAX(safety_stock) FILTER (
                    WHERE lead_time_weeks = 4
                      AND target_service_level = 0.99
                ) AS high_ss
            FROM inv
            GROUP BY id
        )
        SELECT COUNT(*)
        FROM expected e
        JOIN sensitivity s USING (id)
        WHERE s.baseline_reorder_point != e.baseline_rop
           OR s.high_policy_reorder_point != e.high_rop
           OR ABS(
                s.additional_reorder_point_units
                - (e.high_rop - e.baseline_rop)
              ) > 0.0001
           OR ABS(
                s.additional_safety_stock_units
                - (e.high_ss - e.baseline_ss)
              ) > 0.0001
    """).fetchone()[0]

    check("Sensitivity differences match source scenarios",
          mismatches == 0, f"{mismatches} mismatches")

    pct_mismatches = con.execute("""
        SELECT COUNT(*)
        FROM sensitivity
        WHERE ABS(
            reorder_point_increase_pct
            - 100.0 * additional_reorder_point_units
              / NULLIF(baseline_reorder_point, 0)
        ) > 0.02
          AND baseline_reorder_point != 0
    """).fetchone()[0]

    check("Percentage increases calculate correctly",
          pct_mismatches == 0, f"{pct_mismatches} mismatches")

    print(f"\nRESULT: {passed}/{passed + failed} checks passed")
    if failed:
        raise SystemExit(1)

finally:
    con.close()
