from __future__ import annotations

BUCKET = "sales-mini"

def bronze_prefix(table: str) -> str:
    return f"s3://{BUCKET}/bronze/run_ts=20260210T190000Z/{table}"

def silver_uri(table: str) -> str:
    return f"s3://{BUCKET}/silver/{table}/{table}.csv"

def gold_uri(entity: str) -> str:
    # entity examples: dim_customers, dim_products, dim_organizations, fact_sales
    return f"s3://{BUCKET}/gold/{entity}/{entity}.csv"

def watermark_uri(table: str) -> str:
    return f"s3://{BUCKET}/_meta/watermarks/{table}.json"