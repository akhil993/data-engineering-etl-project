from __future__ import annotations

import awswrangler as wr

from common.s3_write import write_single_csv_to_s3
from common.utils import now_utc_str, pick_first_col, standardize_columns, trim_strings

SILVER_PRODUCTS = "s3://XXXXXXX.csv"
GOLD_DIM_PRODUCTS = "s3://XXXXXXX.csv"


def main():
    df = wr.s3.read_csv(SILVER_PRODUCTS)
    df = standardize_columns(df)
    df = trim_strings(df)

    prod_key = pick_first_col(df, ["product_id", "productid", "sku", "item_id"])
    if not prod_key:
        raise ValueError(f"Products missing product_id-like key. Found: {list(df.columns)}")

    df = df.drop_duplicates(subset=[prod_key]).copy()
    df["gold_load_ts"] = now_utc_str()
    df = df.sort_values(prod_key).reset_index(drop=True)

    write_single_csv_to_s3(df, GOLD_DIM_PRODUCTS)


if __name__ == "__main__":
    main()
