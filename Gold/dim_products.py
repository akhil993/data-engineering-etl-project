from __future__ import annotations

import awswrangler as wr
from common.s3_write import write_single_csv_to_s3
from common.utils import standardize_columns, trim_strings, now_utc_str

SILVER_PRODUCTS = "s3://sales-mini/silver/Products/Products.csv"
GOLD_DIM_PRODUCTS = "s3://sales-mini/gold/dim_products/dim_products.csv"


def main():
    df = wr.s3.read_csv(SILVER_PRODUCTS)
    df = trim_strings(standardize_columns(df))

    keep_cols = [
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "list_price",
    ]

    df = df[[c for c in keep_cols if c in df.columns]].copy()
    df["gold_load_ts"] = now_utc_str()

    write_single_csv_to_s3(df, GOLD_DIM_PRODUCTS)


if __name__ == "__main__":
    main()