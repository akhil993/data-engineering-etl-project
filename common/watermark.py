from __future__ import annotations
import json
import awswrangler as wr

def get_watermark(wm_uri: str) -> str | None:
    try:
        obj = wr.s3.read_text(wm_uri)
        return json.loads(obj).get("watermark")
    except Exception:
        return None

def set_watermark(wm_uri: str, watermark: str) -> None:
    wr.s3.to_text(json.dumps({"watermark": watermark}), wm_uri)