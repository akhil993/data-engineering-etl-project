from __future__ import annotations

import awswrangler as wr

from common.s3_write import write_single_csv_to_s3
from common.utils import now_utc_str, pick_first_col, standardize_columns, trim_strings

SILVER_ORGS = "s3://XXXXXXX.csv"
GOLD_DIM_ORGS = "s3://sXXXXXXX.csv"


def main():
    df = wr.s3.read_csv(SILVER_ORGS)
    df = standardize_columns(df)
    df = trim_strings(df)

    org_key = pick_first_col(df, ["orgid", "organization_id", "org_id", "organizationid"])
    if not org_key:
        raise ValueError(f"Organizations missing organization_id-like key. Found: {list(df.columns)}")

    df = df.drop_duplicates(subset=[org_key]).copy()
    df["gold_load_ts"] = now_utc_str()
    df = df.sort_values(org_key).reset_index(drop=True)

    write_single_csv_to_s3(df, GOLD_DIM_ORGS)


if __name__ == "__main__":
    main()
