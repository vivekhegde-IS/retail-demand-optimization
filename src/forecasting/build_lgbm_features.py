from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "data" / "processed" / "gold" / "weekly_forecasting_base.parquet"
OUTPUT = ROOT / "data" / "processed" / "gold" / "weekly_model_features.parquet"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
TEMP.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

try:
    base_path = BASE.as_posix().replace("'", "''")
    output_path = OUTPUT.as_posix().replace("'", "''")

    print("Building lag, rolling-demand and calendar features...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE model_features AS
        WITH history AS (
            SELECT
                id,
                item_id,
                dept_id,
                cat_id,
                store_id,
                state_id,
                week_start,
                weekly_units_sold AS actual_units,

                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY week_start DESC
                ) AS weeks_from_end,

                LAG(weekly_units_sold, 1) OVER w AS lag_1,
                LAG(weekly_units_sold, 2) OVER w AS lag_2,
                LAG(weekly_units_sold, 3) OVER w AS lag_3,
                LAG(weekly_units_sold, 4) OVER w AS lag_4,
                LAG(weekly_units_sold, 8) OVER w AS lag_8,
                LAG(weekly_units_sold, 13) OVER w AS lag_13,
                LAG(weekly_units_sold, 26) OVER w AS lag_26,
                LAG(weekly_units_sold, 52) OVER w AS lag_52,

                AVG(weekly_units_sold) OVER (
                    PARTITION BY id ORDER BY week_start
                    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
                ) AS rolling_mean_4,

                AVG(weekly_units_sold) OVER (
                    PARTITION BY id ORDER BY week_start
                    ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                ) AS rolling_mean_13,

                STDDEV_SAMP(weekly_units_sold) OVER (
                    PARTITION BY id ORDER BY week_start
                    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
                ) AS rolling_std_4

            FROM read_parquet('{base_path}')
            WINDOW w AS (
                PARTITION BY id ORDER BY week_start
            )
        )
        SELECT
            *,
            WEEK(week_start) AS week_of_year,
            MONTH(week_start) AS month,
            YEAR(week_start) - 2011 AS year_index
        FROM history
    """)

    print("Writing features to Parquet...")

    con.execute(f"""
        COPY model_features
        TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print("\nChecking feature table...")

    stats = con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT id) AS series_count,
            COUNT(DISTINCT week_start) AS week_count,
            MIN(week_start) AS first_week,
            MAX(week_start) AS last_week,
            COUNT(*) FILTER (WHERE weeks_from_end <= 28) AS holdout_rows,
            COUNT(*) FILTER (
                WHERE weeks_from_end <= 28 AND lag_52 IS NULL
            ) AS holdout_missing_lag52,
            COUNT(*) FILTER (
                WHERE weeks_from_end > 28 AND lag_52 IS NOT NULL
            ) AS eligible_training_rows
        FROM model_features
    """).fetchone()

    names = [
        "Total rows",
        "Product-store series",
        "Distinct weeks",
        "First week",
        "Last week",
        "Holdout rows",
        "Holdout rows missing lag_52",
        "Training rows with lag_52",
    ]

    for name, value in zip(names, stats):
        print(f"{name}: {value:,}" if isinstance(value, int) else f"{name}: {value}")

    print("\nOutput:", OUTPUT)

finally:
    con.close()
