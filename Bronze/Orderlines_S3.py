"""
Extract from SQL Server using a SQL query and upload results to S3 as Parquet (or CSV).

Requirements:
  pip install pyodbc pandas pyarrow boto3

Notes:
- Uses SQL Authentication (UID/PWD) so it works from macOS too.
- Writes to a timestamped S3 prefix: .../run_ts=YYYYMMDDTHHMMSSZ/
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from io import BytesIO, StringIO
import sys

import boto3
import pandas as pd


# ----------------------------
# CONFIG (env vars + defaults)
# ----------------------------
SQL_SERVER = os.getenv("SQL_SERVER", "XXXXX")
SQL_DATABASE = os.getenv("SQL_DATABASE", "XXXXX")
SQL_USERNAME = os.getenv("SQL_USERNAME", "XXXXX")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "XXXXXXX")  # set env var; don't hardcode
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
folder_name = "XXXXXXXXX"
S3_BUCKET = os.getenv("S3_BUCKET", "XXXXXX")
S3_PREFIX_BASE = os.getenv("S3_PREFIX_BASE", f"XXXXXXXXX/{folder_name}")  # no leading slash
AWS_REGION = os.getenv("AWS_REGION", "XXXXXX")

SQL_QUERY = os.getenv(
    "SQL_QUERY",
    """
    SELECT *
    FROM dbo.XXXXXX
    """
).strip()

OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "csv").lower()  # parquet or csv
CHUNKSIZE = int(os.getenv("CHUNKSIZE", "50000"))  # set 0 to disable chunking


# ----------------------------
# Helpers
# ----------------------------
def build_conn_str() -> str:
    if not SQL_PASSWORD:
        raise ValueError("Missing SQL_PASSWORD env var. Set it before running.")
    return (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )

def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)

def upload_bytes_to_s3(s3, bucket: str, key: str, data: bytes, content_type: str):
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    print(f"Uploaded: s3://{bucket}/{key} ({len(data):,} bytes)")

def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    bio = BytesIO()
    df.to_parquet(bio, index=False)
    return bio.getvalue()

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    sio = StringIO()
    df.to_csv(sio, index=False)
    return sio.getvalue().encode("utf-8")


# ----------------------------
# Main
# ----------------------------
def main():

    import pyodbc

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_prefix = f"{S3_PREFIX_BASE}/"
  
    s3 = s3_client()
    conn_str = build_conn_str()

    print("Connecting to SQL Server...")
    with pyodbc.connect(conn_str) as conn:
        print("Connected. Running query...")

        # Chunked extract (recommended)
        if CHUNKSIZE and CHUNKSIZE > 0:
            reader = pd.read_sql(SQL_QUERY, conn, chunksize=CHUNKSIZE)
            part = 0
            total_rows = 0

            for chunk_df in reader:
                part += 1
                total_rows += len(chunk_df)

                if OUTPUT_FORMAT == "parquet":
                    payload = df_to_parquet_bytes(chunk_df)
                    key = f"{run_prefix}part-{part:05d}.parquet"
                    upload_bytes_to_s3(s3, S3_BUCKET, key, payload, "application/octet-stream")

                elif OUTPUT_FORMAT == "csv":
                    payload = df_to_csv_bytes(chunk_df)
                    key = f"{run_prefix}{folder_name}-{part:05d}.csv"
                    upload_bytes_to_s3(s3, S3_BUCKET, key, payload, "text/csv")

                else:
                    raise ValueError("OUTPUT_FORMAT must be 'parquet' or 'csv'")

            print(f"Done. Uploaded {part} part(s), {total_rows:,} row(s).")

        # Single file extract
        else:
            df = pd.read_sql(SQL_QUERY, conn)
            print(f"Fetched {len(df):,} row(s). Uploading single file...")

            if OUTPUT_FORMAT == "parquet":
                payload = df_to_parquet_bytes(df)
                key = f"{run_prefix}data.parquet"
                upload_bytes_to_s3(s3, S3_BUCKET, key, payload, "application/octet-stream")

            elif OUTPUT_FORMAT == "csv":
                payload = df_to_csv_bytes(df)
                key = f"{run_prefix}Folder.csv"
                upload_bytes_to_s3(s3, S3_BUCKET, key, payload, "text/csv")

            else:
                raise ValueError("OUTPUT_FORMAT must be 'parquet' or 'csv'")

            print("Done.")


if __name__ == "__main__":
    main()
