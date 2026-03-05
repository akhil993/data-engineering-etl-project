from __future__ import annotations

import awswrangler as wr
from common.s3_write import write_single_csv_to_s3
from common.utils import standardize_columns, trim_strings, now_utc_str

SILVER_CUSTOMERS = "s3://sales-mini/silver/Customers/Customers.csv"
GOLD_DIM_CUSTOMERS = "s3://sales-mini/gold/dim_customers/dim_customers.csv"


def main():
    df = wr.s3.read_csv(SILVER_CUSTOMERS)
    df = trim_strings(standardize_columns(df))

    keep_cols = [
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "country",
    ]

    df = df[[c for c in keep_cols if c in df.columns]].copy()
    df["gold_load_ts"] = now_utc_str()

    write_single_csv_to_s3(df, GOLD_DIM_CUSTOMERS)


if __name__ == "__main__":
    main()