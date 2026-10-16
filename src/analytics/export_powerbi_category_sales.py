from pathlib import Path
import duckdb

root = Path(__file__).resolve().parents[2]
source = root / "data" / "processed" / "gold" / "monthly_demand_trends.parquet"
destination = root / "data" / "outputs" / "powerbi" / "category_sales.csv"
destination.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
try:
    src = source.as_posix().replace("'", "''")
    dst = destination.as_posix().replace("'", "''")

    con.execute(f"""
        COPY (
            SELECT
                cat_id,
                SUM(units_sold) AS total_units_sold,
                SUM(estimated_revenue) AS total_estimated_revenue
            FROM read_parquet('{src}')
            GROUP BY cat_id
            ORDER BY total_units_sold DESC
        )
        TO '{dst}' (FORMAT CSV, HEADER)
    """)

    print(con.execute(
        f"SELECT * FROM read_csv_auto('{dst}')"
    ).fetchdf().to_string(index=False))
    print(f"\nSaved: {destination}")
finally:
    con.close()
