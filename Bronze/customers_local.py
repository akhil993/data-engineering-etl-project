from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
datetime.now(timezone.utc)
#--- logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s")
def upsert_append_bronze_csv(bronze_file: str | Path, new_rows: list[dict]) -> None:
  bronze_file = Path("Data/bronze/Customers.csv")
  bronze_file.parent.mkdir(parents=True, exist_ok=True)
  
  df_new = pd.DataFrame(new_rows)
  current_ts= datetime.now(timezone.utc).isoformat()
  df_new["Created_date"] = current_ts
  df_new["Updated_date"] = current_ts
  if bronze_file.exists():
    df_new.to_csv(bronze_file, mode="a", header=False, index=False)
  else:
     df_new.to_csv(bronze_file, mode="w", header=True, index=False)
  print(f"Processed {len(df_new)} rows into {bronze_file}") 
new_rows= [
  {"customer_id":1
   ,"customer_name":"John Doe",
   "email":"john.doe@example.com","city":"New York","state":"NY","country":"USA","signup_date":"2023-01-01"} ]
upsert_append_bronze_csv("Data/bronze/Customers.csv", new_rows)

