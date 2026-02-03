from __future__ import annotations

import awswrangler as wr

from common.s3_write import write_single_csv_to_s3
from common.utils import now_utc_str, pick_first_col, standardize_columns, trim_strings

SILVER_CUSTOMERS = "s3://XXXXXXX.csv"
GOLD_DIM_CUSTOMERS = "s3://sXXXXXXX.csv"


def main():
    df = wr.s3.read_csv(SILVER_CUSTOMERS)
    df = standardize_columns(df)
    df = trim_strings(df)

    cust_key = pick_first_col(df, ["customer_id", "customerid", "cust_id"])
    if not cust_key:
        raise ValueError(f"Customers missing customer_id-like key. Found: {list(df.columns)}")

    df = df.drop_duplicates(subset=[cust_key]).copy()
    df["gold_load_ts"] = now_utc_str()
    df = df.sort_values(cust_key).reset_index(drop=True)

    write_single_csv_to_s3(df, GOLD_DIM_CUSTOMERS)


if __name__ == "__main__":
    main()
