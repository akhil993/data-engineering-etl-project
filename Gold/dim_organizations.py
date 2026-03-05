from __future__ import annotations

import awswrangler as wr
from common.s3_write import write_single_csv_to_s3
from common.utils import standardize_columns, trim_strings, now_utc_str

SILVER_ORGS = "s3://sales-mini/silver/Organizations/Organizations.csv"
GOLD_DIM_ORGS = "s3://sales-mini/gold/dim_organizations/dim_organizations.csv"


def main():
    df = wr.s3.read_csv(SILVER_ORGS)
    df = trim_strings(standardize_columns(df))

    keep_cols = [
        "orgid",
        "orgcode",
        "orgname",
        "orgtype",
        "city",
        "state",
        "country",
    ]

    df = df[[c for c in keep_cols if c in df.columns]].copy()
    df["gold_load_ts"] = now_utc_str()

    write_single_csv_to_s3(df, GOLD_DIM_ORGS)


if __name__ == "__main__":
    main()