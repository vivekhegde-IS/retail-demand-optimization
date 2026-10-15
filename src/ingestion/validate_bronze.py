from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
DB = PROJECT / "data" / "bronze" / "retail_bronze.duckdb"

checks = {
    "Calendar keys": """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT d) AS unique_days,
               COUNT(DISTINCT date) AS unique_dates,
               COUNT(*) FILTER (
                   WHERE d IS NULL OR date IS NULL
               ) AS null_keys
        FROM bronze_calendar
    """,
    "Validation sales IDs": """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT id) AS unique_ids,
               COUNT(*) FILTER (
                   WHERE id IS NULL OR item_id IS NULL
                      OR store_id IS NULL
               ) AS null_keys
        FROM bronze_sales_validation
    """,
    "Evaluation sales IDs": """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT id) AS unique_ids,
               COUNT(*) FILTER (
                   WHERE id IS NULL OR item_id IS NULL
                      OR store_id IS NULL
               ) AS null_keys
        FROM bronze_sales_evaluation
    """,
    "Duplicate price keys": """
        SELECT COUNT(*) AS duplicate_key_groups
        FROM (
            SELECT store_id, item_id, wm_yr_wk
            FROM bronze_sell_prices
            GROUP BY store_id, item_id, wm_yr_wk
            HAVING COUNT(*) > 1
        ) duplicates
    """,
    "Price quality": """
        SELECT
            COUNT(*) FILTER (
                WHERE store_id IS NULL OR item_id IS NULL
                   OR wm_yr_wk IS NULL
            ) AS null_keys,
            COUNT(*) FILTER (
                WHERE sell_price IS NULL
            ) AS null_prices,
            COUNT(*) FILTER (
                WHERE sell_price <= 0
            ) AS non_positive_prices
        FROM bronze_sell_prices
    """,
    "Submission IDs": """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT id) AS unique_ids,
               COUNT(*) FILTER (
                   WHERE id IS NULL
               ) AS null_ids
        FROM bronze_sample_submission
    """
}

con = duckdb.connect(str(DB), read_only=True)

try:
    for name, query in checks.items():
        print(f"\n--- {name} ---")
        result = con.execute(query)
        print([column[0] for column in result.description])
        print(result.fetchone())
finally:
    con.close()
