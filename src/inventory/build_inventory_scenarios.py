from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
WEEKLY = ROOT / "data" / "processed" / "gold" / "weekly_forecasting_base.parquet"
FORECASTS = ROOT / "data" / "outputs" / "forecasting" / "lightgbm_forecasts.parquet"
OUT_DIR = ROOT / "data" / "processed" / "gold"
OUT = OUT_DIR / "inventory_scenarios.parquet"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

OUT_DIR.mkdir(parents=True, exist_ok=True)
TEMP.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

try:
    weekly_path = WEEKLY.as_posix().replace("'", "''")
    forecast_path = FORECASTS.as_posix().replace("'", "''")
    output_path = OUT.as_posix().replace("'", "''")

    print("Identifying the holdout boundary...")

    holdout_start, latest_forecast = con.execute(f"""
        SELECT MIN(week_start), MAX(week_start)
        FROM read_parquet('{forecast_path}')
    """).fetchone()

    print("Holdout start:", holdout_start)
    print("Latest forecast week:", latest_forecast)

    print("\nCalculating historical demand statistics...")
    print("Only history before the holdout start is used for variability.")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE historical_stats AS
        SELECT
            id,
            ANY_VALUE(item_id) AS item_id,
            ANY_VALUE(dept_id) AS dept_id,
            ANY_VALUE(cat_id) AS cat_id,
            ANY_VALUE(store_id) AS store_id,
            ANY_VALUE(state_id) AS state_id,
            COUNT(*) AS history_weeks,
            AVG(weekly_units_sold) AS historical_mean_weekly_demand,
            COALESCE(STDDEV_SAMP(weekly_units_sold), 0)
                AS historical_std_weekly_demand
        FROM read_parquet('{weekly_path}')
        WHERE week_start < DATE '{holdout_start:%Y-%m-%d}'
        GROUP BY id
    """)

    print("Selecting the latest rolling forecast per product-store...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE latest_forecasts AS
        SELECT id, week_start AS forecast_week,
               forecast_units AS forecast_weekly_demand
        FROM read_parquet('{forecast_path}')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id ORDER BY week_start DESC
        ) = 1
    """)

    print("Building 9 scenarios per product-store...")

    con.execute(f"""
        COPY (
            WITH assumptions AS (
                SELECT * FROM (
                    VALUES
                        (1, 0.90, 1.2815515655),
                        (1, 0.95, 1.6448536270),
                        (1, 0.99, 2.3263478740),
                        (2, 0.90, 1.2815515655),
                        (2, 0.95, 1.6448536270),
                        (2, 0.99, 2.3263478740),
                        (4, 0.90, 1.2815515655),
                        (4, 0.95, 1.6448536270),
                        (4, 0.99, 2.3263478740)
                ) AS t(lead_time_weeks, target_service_level, z_score)
            ),
            joined AS (
                SELECT
                    h.*,
                    f.forecast_week,
                    GREATEST(f.forecast_weekly_demand, 0)
                        AS forecast_weekly_demand
                FROM historical_stats h
                JOIN latest_forecasts f USING (id)
            )
            SELECT
                j.id,
                j.item_id,
                j.dept_id,
                j.cat_id,
                j.store_id,
                j.state_id,
                j.forecast_week,
                j.history_weeks,
                j.historical_mean_weekly_demand,
                j.historical_std_weekly_demand,
                j.forecast_weekly_demand,
                a.lead_time_weeks,
                a.target_service_level,
                a.z_score,

                j.forecast_weekly_demand * a.lead_time_weeks
                    AS lead_time_demand,

                a.z_score
                    * j.historical_std_weekly_demand
                    * SQRT(CAST(a.lead_time_weeks AS DOUBLE))
                    AS safety_stock,

                CEIL(
                    j.forecast_weekly_demand * a.lead_time_weeks
                    + a.z_score
                    * j.historical_std_weekly_demand
                    * SQRT(CAST(a.lead_time_weeks AS DOUBLE))
                ) AS reorder_point_units

            FROM joined j
            CROSS JOIN assumptions a
        )
        TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print("\nINVENTORY SCENARIO SUMMARY")

    summary = con.execute(f"""
        SELECT
            COUNT(*) AS scenario_rows,
            COUNT(DISTINCT id) AS product_store_series,
            COUNT(DISTINCT lead_time_weeks) AS lead_time_scenarios,
            COUNT(DISTINCT target_service_level) AS service_level_scenarios,
            COUNT(*) FILTER (
                WHERE reorder_point_units < 0
                   OR safety_stock < 0
                   OR lead_time_demand < 0
            ) AS invalid_rows,
            MIN(reorder_point_units) AS min_reorder_point,
            MAX(reorder_point_units) AS max_reorder_point
        FROM read_parquet('{output_path}')
    """).fetchone()

    for name, value in zip([
        "Scenario rows",
        "Product-store series",
        "Lead-time scenarios",
        "Service-level scenarios",
        "Invalid rows",
        "Minimum reorder point",
        "Maximum reorder point",
    ], summary):
        print(f"{name}: {value:,}" if isinstance(value, int) else f"{name}: {value}")

    print("\nEXAMPLE: 2-WEEK LEAD TIME, 95% SERVICE LEVEL")

    example = con.execute(f"""
        SELECT
            id, cat_id, store_id,
            forecast_weekly_demand,
            historical_std_weekly_demand,
            lead_time_demand,
            safety_stock,
            reorder_point_units
        FROM read_parquet('{output_path}')
        WHERE lead_time_weeks = 2
          AND target_service_level = 0.95
        ORDER BY reorder_point_units DESC
        LIMIT 10
    """).df()

    print(example.to_string(index=False))
    print("\nSaved:", OUT)

finally:
    con.close()
