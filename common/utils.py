from __future__ import annotations

from datetime import datetime, timezone
import pandas as pd


def now_utc_str() -> str:
    """Return current UTC timestamp as string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to snake_case lowercase."""
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df


def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace from string columns, keep NaN intact."""
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].where(df[col].isna(), df[col].astype(str).str.strip())
    return df


def pick_first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column that exists in df from candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def to_numeric_safe(s: pd.Series) -> pd.Series:
    """Convert to numeric, coercing errors to NaN."""
    return pd.to_numeric(s, errors="coerce")
