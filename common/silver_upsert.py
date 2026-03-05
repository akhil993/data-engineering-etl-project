from __future__ import annotations

import re
from datetime import datetime, timezone

import awswrangler as wr
import pandas as pd


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"[^a-z0-9]+", "_", c.strip().lower()) for c in df.columns]
    return df


def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str).str.strip()
    return df


def pick_first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = set(df.columns)
    for c in candidates:
        c = c.lower()
        if c in cols:
            return c
    return None


def upsert(existing: pd.DataFrame, incoming: pd.DataFrame, pk: str, updated_col: str | None) -> pd.DataFrame:
    base = pd.concat([existing, incoming], ignore_index=True) if not existing.empty else incoming.copy()

    # prefer updated_col if available (keeps most recent record per PK)
    if updated_col and updated_col in base.columns:
        base[updated_col] = pd.to_datetime(base[updated_col], errors="coerce", utc=True)
        base = base.sort_values([pk, updated_col]).drop_duplicates(pk, keep="last")
        base[updated_col] = base[updated_col].dt.strftime("%Y-%m-%d %H:%M:%SZ")
    else:
        base = base.drop_duplicates(pk, keep="last")

    base["silver_load_ts"] = now_utc_str()
    return base


def bronze_run_to_silver(
    bronze_run_prefix: str,
    bronze_filename: str,
    silver_uri: str,
    pk_candidates: list[str],
    updated_candidates: list[str] | None = None,
) -> None:
    updated_candidates = updated_candidates or []

    bronze_uri = f"{bronze_run_prefix}/{bronze_filename}"
    incoming = wr.s3.read_csv(bronze_uri)
    incoming = trim_strings(standardize_columns(incoming))

    pk = pick_first_col(incoming, pk_candidates)
    upd = pick_first_col(incoming, updated_candidates)

    if not pk:
        raise ValueError(f"{bronze_filename}: missing PK. Found columns: {list(incoming.columns)}")

    try:
        existing = wr.s3.read_csv(silver_uri)
        existing = trim_strings(standardize_columns(existing))
    except Exception:
        existing = pd.DataFrame()

    merged = upsert(existing, incoming, pk=pk, updated_col=upd)
    wr.s3.to_csv(merged, silver_uri, index=False, dataset=False)
    print(f"✅ Upserted {bronze_filename} -> {silver_uri} | rows={len(merged):,}")