from __future__ import annotations

from datetime import datetime, timezone

import awswrangler as wr
import pandas as pd

# =========================
# CONFIG
# =========================
Table_name = "Products"
BRONZE_PATH = f"s3://sXXXXXXX/{Table_name}/"
SILVER_PATH = f"s3://sXXXXXXX/{Table_name}/{Table_name}.csv"

# If you want to only process the *latest run_ts*, set this True.
PROCESS_LATEST_RUN_ONLY = True

# =========================
# HELPERS
# =========================
def get_latest_run_ts_prefix(bronze_path: str) -> str:
    """
    Finds the latest run_ts=... folder under the bronze_path and returns its full prefix.
    Example return:
      s3://bucket/bronze/.../customers/run_ts=20260127T184455Z/
    """
    # List immediate "directories" under bronze_path
    prefixes = wr.s3.list_directories(bronze_path)

    run_prefixes = [p for p in prefixes if "run_ts=" in p]
    if not run_prefixes:
        # If there are no run_ts folders, just read directly from BRONZE_PATH
        return bronze_path

    # Lexicographic sort works because timestamp format is sortable
    run_prefixes.sort()
    return run_prefixes[-1]

# =========================
# MAIN
# =========================
def main():
    bronze_read_path = BRONZE_PATH

    if PROCESS_LATEST_RUN_ONLY:
        bronze_read_path = get_latest_run_ts_prefix(BRONZE_PATH)

    print(f"Reading Bronze CSV from: {bronze_read_path}")

    # Read all CSV files from the chosen path
    df = wr.s3.read_csv(
        path=bronze_read_path,
        dataset=True  # reads all files under the prefix
    )

    print(f"Rows read from Bronze: {len(df):,}")

    # -------------------------
    # STANDARDIZE COLUMN NAMES
    # -------------------------
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # -------------------------
    # BASIC CLEANING
    # -------------------------
    # Trim strings
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    # Deduplicate
    if "customer_id" in df.columns:
        df = df.drop_duplicates(subset=["customer_id"])
    else:
        df = df.drop_duplicates()

    # Add ingestion timestamp
    df["ingestion_ts"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

    print(f"Rows after cleaning: {len(df):,}")

    # -------------------------
    # WRITE SILVER AS CSV
    # -------------------------
    print(f"Writing Silver CSV to: {SILVER_PATH}")
    wr.s3.delete_objects(SILVER_PATH)
    wr.s3.to_csv(
        df=df,
        path=SILVER_PATH,
        dataset=False, 
        index=False
    )

    print("✅ Silver customers (CSV) written successfully")

if __name__ == "__main__":
    main()
