from pathlib import Path
import duckdb
import lightgbm as lgb
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
FEATURES_FILE = ROOT / "data" / "processed" / "gold" / "weekly_model_features.parquet"
OUT = ROOT / "data" / "outputs" / "forecasting"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

OUT.mkdir(parents=True, exist_ok=True)
TEMP.mkdir(parents=True, exist_ok=True)

FORECAST_FILE = OUT / "lightgbm_forecasts.parquet"
METRICS_FILE = OUT / "lightgbm_model_metrics.parquet"
MODEL_FILE = OUT / "lightgbm_weekly_model.txt"

CAT_COLS = ["item_id", "dept_id", "cat_id", "store_id", "state_id"]
NUM_COLS = [
    "lag_1", "lag_2", "lag_3", "lag_4",
    "lag_8", "lag_13", "lag_26", "lag_52",
    "rolling_mean_4", "rolling_mean_13", "rolling_std_4",
    "week_of_year", "month", "year_index",
]
FEATURE_COLS = CAT_COLS + NUM_COLS

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

try:
    feature_path = FEATURES_FILE.as_posix().replace("'", "''")

    print("Loading a deterministic training sample...")
    train = con.execute(f"""
        SELECT {", ".join(FEATURE_COLS)}, actual_units
        FROM read_parquet('{feature_path}')
        WHERE weeks_from_end > 28
          AND lag_52 IS NOT NULL
          AND hash(id || '|' || CAST(week_start AS VARCHAR)) % 3 = 0
    """).df()

    print(f"Training sample rows: {len(train):,}")

    print("Loading the 28-week rolling holdout...")
    test = con.execute(f"""
        SELECT id, week_start, actual_units, {", ".join(FEATURE_COLS)}
        FROM read_parquet('{feature_path}')
        WHERE weeks_from_end <= 28
        ORDER BY week_start, id
    """).df()

    print(f"Holdout rows: {len(test):,}")

    print("Encoding categorical features...")
    for col in CAT_COLS:
        categories = sorted(train[col].dropna().unique().tolist())
        train[col] = pd.Categorical(train[col], categories=categories)
        test[col] = pd.Categorical(test[col], categories=categories)

    X_train = train[FEATURE_COLS]
    y_train = train["actual_units"].astype("float32")
    X_test = test[FEATURE_COLS]

    del train

    print("Training LightGBM...")
    model = lgb.LGBMRegressor(
        objective="regression",
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        max_bin=63,
        min_child_samples=100,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=2,
        verbosity=-1,
        force_col_wise=True,
    )

    model.fit(
        X_train,
        y_train,
        categorical_feature=CAT_COLS,
    )

    print("Saving trained model...")
    model.booster_.save_model(str(MODEL_FILE))

    print("Predicting holdout in batches...")
    predictions = np.empty(len(test), dtype=np.float32)
    batch_size = 100_000

    for start in range(0, len(test), batch_size):
        end = min(start + batch_size, len(test))
        predictions[start:end] = model.predict(
            X_test.iloc[start:end],
            num_threads=2,
        )
        print(f"Predicted {end:,} / {len(test):,}")

    # Demand cannot be negative.
    predictions = np.maximum(predictions, 0)

    forecasts = pd.DataFrame({
        "id": test["id"],
        "week_start": test["week_start"],
        "actual_units": test["actual_units"],
        "model": "lightgbm",
        "forecast_units": predictions,
    })

    forecasts.to_parquet(FORECAST_FILE, index=False, compression="zstd")

    errors = (
        forecasts["actual_units"].to_numpy(dtype=np.float64)
        - forecasts["forecast_units"].to_numpy(dtype=np.float64)
    )
    actual = forecasts["actual_units"].to_numpy(dtype=np.float64)
    predicted = forecasts["forecast_units"].to_numpy(dtype=np.float64)

    metrics = pd.DataFrame([{
        "model": "lightgbm",
        "evaluated_forecasts": len(forecasts),
        "mae": float(np.mean(np.abs(errors))),
        "wape_pct": float(100 * np.sum(np.abs(errors)) / np.sum(actual)),
        "rmse": float(np.sqrt(np.mean(errors ** 2))),
        "total_actual_units": float(np.sum(actual)),
        "total_forecast_units": float(np.sum(predicted)),
    }])

    metrics.to_parquet(METRICS_FILE, index=False, compression="zstd")

    print("\nLIGHTGBM RESULTS")
    print(metrics.to_string(index=False))
    print("\nTop feature importances:")
    importance = pd.DataFrame({
        "feature": FEATURE_COLS,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)
    print(importance.head(10).to_string(index=False))

    print("\nSaved forecasts:", FORECAST_FILE)
    print("Saved metrics:", METRICS_FILE)
    print("Saved model:", MODEL_FILE)

finally:
    con.close()
