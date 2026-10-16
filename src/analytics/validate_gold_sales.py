from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
GOLD = PROJECT / "data" / "processed" / "gold"
SPILL = PROJECT / "data" / "temp" / "duckdb_spill"

FILES = {
    "weekly": GOLD / "weekly_item_store.parquet",
    "monthly": GOLD / "monthly_store_category.parquet",
    "store": GOLD / "store_performance.parquet",
}

def sql_path(path):
    return path.resolve().as_posix().replace("'", "''")

def main():
    for path in FILES.values():
        if not path.exists():
            raise FileNotFoundError(f"Missing Gold output: {path}")

    SPILL.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    try:
        con.execute("SET memory_limit = '4GB'")
        con.execute("SET threads = 2")
        con.execute("SET preserve_insertion_order = false")
        con.execute(f"SET temp_directory = '{sql_path(SPILL)}'")
        con.execute("SET max_temp_directory_size = '50GB'")

        for name, path in FILES.items():
            con.execute(
                f"CREATE VIEW {name} AS "
                f"SELECT * FROM read_parquet('{sql_path(path)}')"
            )

        print("Checking Gold aggregate integrity...")

        # Check totals and expected output sizes.
        metrics = con.execute("""
            SELECT
                (SELECT COUNT(*) FROM weekly),
                (SELECT COUNT(*) FROM monthly),
                (SELECT COUNT(*) FROM store),
                (SELECT SUM(weekly_units_sold) FROM weekly),
                (SELECT SUM(units_sold) FROM monthly),
                (SELECT SUM(total_units_sold) FROM store),
                (SELECT SUM(weekly_revenue) FROM weekly),
                (SELECT SUM(revenue) FROM monthly),
                (SELECT SUM(total_revenue) FROM store)
        """).fetchone()

        (
            weekly_rows, monthly_rows, store_rows,
            weekly_units, monthly_units, store_units,
            weekly_revenue, monthly_revenue, store_revenue
        ) = metrics

        print(f"Weekly rows: {weekly_rows:,}")
        print(f"Monthly rows: {monthly_rows:,}")
        print(f"Store rows: {store_rows:,}")
        print(f"Weekly units: {weekly_units:,.0f}")
        print(f"Monthly units: {monthly_units:,.0f}")
        print(f"Store-summary units: {store_units:,.0f}")
        print(f"Weekly revenue: {weekly_revenue:,.2f}")
        print(f"Monthly revenue: {monthly_revenue:,.2f}")
        print(f"Store-summary revenue: {store_revenue:,.2f}")

        checks = {
            "Expected weekly row count": weekly_rows == 8_476_220,
            "Expected monthly row count": monthly_rows == 4_550,
            "Expected store count": store_rows == 10,
            "Weekly units reconcile": weekly_units == 66_927_173,
            "Monthly units reconcile": monthly_units == 66_927_173,
            "Store units reconcile": store_units == 66_927_173,
            "Weekly and monthly revenue reconcile":
                abs(weekly_revenue - monthly_revenue) < 0.01,
            "Monthly and store revenue reconcile":
                abs(monthly_revenue - store_revenue) < 0.01,
        }

        # Check weekly grouping key uniqueness.
        print("\nChecking weekly key uniqueness...")
        duplicate_keys = con.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT id, wm_yr_wk
                FROM weekly
                GROUP BY id, wm_yr_wk
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        checks["No duplicate weekly keys"] = duplicate_keys == 0
        print(f"Duplicate weekly keys: {duplicate_keys:,}")

        # Check for invalid negative quantities or prices.
        invalid = con.execute("""
            SELECT COUNT(*)
            FROM weekly
            WHERE weekly_units_sold < 0
               OR (average_sell_price IS NOT NULL
                   AND average_sell_price <= 0)
        """).fetchone()[0]

        checks["No invalid weekly units or prices"] = invalid == 0
        print(f"Invalid weekly records: {invalid:,}")

        print("\nGOLD VALIDATION RESULTS")
        for name, passed in checks.items():
            print(f"{'PASS' if passed else 'FAIL'}: {name}")

        if not all(checks.values()):
            raise SystemExit("Gold validation failed.")

        print("\nAll Gold aggregation checks passed.")

    finally:
        con.close()

if __name__ == "__main__":
    main()
