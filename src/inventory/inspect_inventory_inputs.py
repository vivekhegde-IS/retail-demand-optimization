from pathlib import Path
import duckdb

root = Path.cwd()
forecast = root / "data" / "outputs" / "forecasting" / "lightgbm_forecasts.parquet"
weekly = root / "data" / "processed" / "gold" / "weekly_forecasting_base.parquet"

con = duckdb.connect()

for label, path in [("LIGHTGBM FORECASTS", forecast), ("WEEKLY HISTORY", weekly)]:
    print(f"\n--- {label} ---")
    path_sql = path.as_posix().replace("'", "''")
    print(con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path_sql}')"
    ).df().to_string(index=False))

    print("\nSample rows:")
    print(con.execute(
        f"SELECT * FROM read_parquet('{path_sql}') LIMIT 3"
    ).df().to_string(index=False))

con.close()
