from __future__ import annotations

from io import StringIO
from urllib.parse import urlparse

import boto3
import pandas as pd


def write_single_csv_to_s3(df: pd.DataFrame, s3_uri: str) -> None:
    """
    Writes exactly ONE CSV object to S3 with the exact key you provide.
    Avoids dataset mode & auto-generated names.
    """
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    buf = StringIO()
    df.to_csv(buf, index=False)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    print(f"✅ wrote {len(df):,} rows -> {s3_uri}")
