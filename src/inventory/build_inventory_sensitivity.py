from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
INPUT = ROOT / "data" / "processed" / "gold" / "inventory_scenarios.parquet"
OUTPUT = ROOT / "data" / "processed" / "gold"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

OUTPUT.mkdir(parents=True, exist_ok=True)
TEMP.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

try:
    source = INPUT.as_posix().replace("'", "''")
    summary_path = (OUTPUT / "inventory_scenario_summary.parquet").as_posix()
    sensitivity_path = (OUTPUT / "inventory_sensitivity.parquet").as_posix()

    print("Loading validated inventory scenarios...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE inv AS
        SELECT * FROM read_parquet('{source}')
    """)

    print("\nSummarizing all nine inventory policies...")
    con.execute(f"""
        COPY (
            SELECT
                lead_time_weeks,
                target_service_level,
                COUNT(*) AS product_store_series,
                ROUND(AVG(forecast_weekly_demand), 2)
                    AS avg_forecast_weekly_demand,
                ROUND(AVG(lead_time_demand), 2)
                    AS avg_lead_time_demand,
                ROUND(AVG(safety_stock), 2)
                    AS avg_safety_stock,
                ROUND(MEDIAN(safety_stock), 2)
                    AS median_safety_stock,
                ROUND(AVG(reorder_point_units), 2)
                    AS avg_reorder_point_units,
                ROUND(MEDIAN(reorder_point_units), 2)
                    AS median_reorder_point_units,
                SUM(reorder_point_units)
                    AS total_reorder_point_units
            FROM inv
            GROUP BY lead_time_weeks, target_service_level
            ORDER BY lead_time_weeks, target_service_level
        ) TO '{summary_path}' (FORMAT PARQUET)
    """)

    print("Comparing the low-policy and high-policy scenarios...")
    con.execute(f"""
        COPY (
            WITH policy_comparison AS (
                SELECT
                    id,
                    ANY_VALUE(item_id) AS item_id,
                    ANY_VALUE(dept_id) AS dept_id,
                    ANY_VALUE(cat_id) AS cat_id,
                    ANY_VALUE(store_id) AS store_id,
                    ANY_VALUE(state_id) AS state_id,

                    MAX(reorder_point_units) FILTER (
                        WHERE lead_time_weeks = 1
                          AND target_service_level = 0.90
                    ) AS baseline_reorder_point,

                    MAX(reorder_point_units) FILTER (
                        WHERE lead_time_weeks = 4
                          AND target_service_level = 0.99
                    ) AS high_policy_reorder_point,

                    MAX(safety_stock) FILTER (
                        WHERE lead_time_weeks = 1
                          AND target_service_level = 0.90
                    ) AS baseline_safety_stock,

                    MAX(safety_stock) FILTER (
                        WHERE lead_time_weeks = 4
                          AND target_service_level = 0.99
                    ) AS high_policy_safety_stock,

                    MAX(forecast_weekly_demand)
                        AS forecast_weekly_demand
                FROM inv
                GROUP BY id
            )
            SELECT
                *,
                high_policy_reorder_point
                    - baseline_reorder_point
                    AS additional_reorder_point_units,

                ROUND(
                    100.0 * (
                        high_policy_reorder_point
                        - baseline_reorder_point
                    ) / NULLIF(baseline_reorder_point, 0),
                    2
                ) AS reorder_point_increase_pct,

                high_policy_safety_stock
                    - baseline_safety_stock
                    AS additional_safety_stock_units
            FROM policy_comparison
            ORDER BY additional_reorder_point_units DESC, id
        ) TO '{sensitivity_path}' (FORMAT PARQUET)
    """)

    print("\nINVENTORY SCENARIO SUMMARY")
    print(con.execute(f"""
        SELECT * FROM read_parquet('{summary_path}')
        ORDER BY lead_time_weeks, target_service_level
    """).fetchdf().to_string(index=False))

    print("\nTOP 10 SERIES BY ADDITIONAL REORDER-POINT UNITS")
    print(con.execute(f"""
        SELECT
            id,
            cat_id,
            store_id,
            baseline_reorder_point,
            high_policy_reorder_point,
            additional_reorder_point_units,
            reorder_point_increase_pct,
            additional_safety_stock_units
        FROM read_parquet('{sensitivity_path}')
        ORDER BY additional_reorder_point_units DESC, id
        LIMIT 10
    """).fetchdf().to_string(index=False))

    print("\nOUTPUT CHECK")
    for filename in [
        "inventory_scenario_summary.parquet",
        "inventory_sensitivity.parquet",
    ]:
        file = OUTPUT / filename
        print(f"{filename}: {file.stat().st_size / (1024 * 1024):.2f} MB")

    print("\nInventory sensitivity outputs created successfully.")

finally:
    con.close()
