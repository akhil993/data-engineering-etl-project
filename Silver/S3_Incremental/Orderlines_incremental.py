from __future__ import annotations

import awswrangler as wr
import pandas as pd

from common.silver_upsert import (
    standardize_columns,
    trim_strings,
    pick_first_col,
    upsert,
)

RUN_PREFIX = "s3://sales-mini/bronze/run_ts=20260210T190000Z"
BRONZE_FILE = "OrderLines.csv"
SILVER_URI = "s3://sales-mini/silver/Orderlines/Orderlines.csv"
PK_CANDIDATES = ["order_line_id", "orderline_id", "line_id"]
UPDATED_CANDIDATES = ["updatedat", "updated_at", "ingestion_ts"]


def main():
    bronze_uri = f"{RUN_PREFIX}/{BRONZE_FILE}"
    incoming = wr.s3.read_csv(bronze_uri)
    incoming = trim_strings(standardize_columns(incoming))

    pk = pick_first_col(incoming, PK_CANDIDATES)
    upd = pick_first_col(incoming, UPDATED_CANDIDATES)

    if not pk:
        raise ValueError(f"Orderlinea: missing PK. Found columns: {list(incoming.columns)}")

    try:
        existing = wr.s3.read_csv(SILVER_URI)
        existing = trim_strings(standardize_columns(existing))
    except Exception:
        existing = pd.DataFrame()

    merged = upsert(existing, incoming, pk=pk, updated_col=upd)
    wr.s3.to_csv(merged, SILVER_URI, index=False, dataset=False)
    print(f"✅ Orderlines upserted -> {SILVER_URI} | rows={len(merged):,}")


if __name__ == "__main__":
    main()