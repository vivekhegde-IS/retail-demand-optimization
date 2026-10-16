from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT / "data" / "bronze" / "retail_bronze.duckdb"
OUT_DIR = PROJECT / "data" / "processed" / "silver"

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)

    calendar_out = (OUT_DIR / "calendar.parquet").as_posix().replace("'", "''")
    prices_out = (OUT_DIR / "sell_prices.parquet").as_posix().replace("'", "''")

    try:
        print("Building Silver calendar...")
        con.execute(f"""
            COPY (
                SELECT
                    CAST(date AS DATE) AS date,
                    CAST(wm_yr_wk AS INTEGER) AS wm_yr_wk,
                    weekday,
                    CAST(wday AS INTEGER) AS wday,
                    CAST(month AS INTEGER) AS month,
                    CAST(year AS INTEGER) AS year,
                    d,
                    event_name_1,
                    event_type_1,
                    event_name_2,
                    event_type_2,
                    CAST(snap_CA AS INTEGER) AS snap_CA,
                    CAST(snap_TX AS INTEGER) AS snap_TX,
                    CAST(snap_WI AS INTEGER) AS snap_WI
                FROM bronze_calendar
            )
            TO '{calendar_out}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        print("Building Silver sell prices...")
        con.execute(f"""
            COPY (
                SELECT
                    store_id,
                    item_id,
                    CAST(wm_yr_wk AS INTEGER) AS wm_yr_wk,
                    CAST(sell_price AS DOUBLE) AS sell_price
                FROM bronze_sell_prices
            )
            TO '{prices_out}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        print("\nSilver dimension row counts:")
        for name, path in [
            ("Calendar", calendar_out),
            ("Sell prices", prices_out),
        ]:
            count = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{path}')"
            ).fetchone()[0]
            print(f"{name}: {count:,}")

        print(f"\nCalendar output: {OUT_DIR / 'calendar.parquet'}")
        print(f"Price output: {OUT_DIR / 'sell_prices.parquet'}")

    finally:
        con.close()

if __name__ == "__main__":
    main()
