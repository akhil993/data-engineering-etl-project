from __future__ import annotations

import awswrangler as wr
import pandas as pd

from common.s3_write import write_single_csv_to_s3
from common.utils import (
    standardize_columns,
    trim_strings,
    pick_first_col,
    to_numeric_safe,
    now_utc_str,
)

SILVER_ORDERS = "s3://sales-mini/silver/Orders/Orders.csv"
SILVER_LINES = "s3://sales-mini/silver/Orderlines/Orderlines.csv"
GOLD_FACT_SALES = "s3://sales-mini/gold/fact_sales/fact_sales.csv"


def main():
    orders = wr.s3.read_csv(SILVER_ORDERS)
    lines = wr.s3.read_csv(SILVER_LINES)

    orders = trim_strings(standardize_columns(orders))
    lines = trim_strings(standardize_columns(lines))

    order_id_o = pick_first_col(orders, ["order_id", "orderid"])
    order_id_l = pick_first_col(lines, ["order_id", "orderid"])

    if not order_id_o or not order_id_l:
        raise ValueError("order_id missing in Orders or Orderlines")

    qty = pick_first_col(lines, ["quantity", "qty"])
    price = pick_first_col(lines, ["unit_price", "price"])
    discount = pick_first_col(lines, ["discountamount", "discount"])

    sales_amount = to_numeric_safe(lines[qty]) * to_numeric_safe(lines[price])

    if discount:
        sales_amount = sales_amount - to_numeric_safe(lines[discount]).fillna(0)

    lines["sales_amount"] = sales_amount.fillna(0)

    join_cols = [
        order_id_o,
        pick_first_col(orders, ["customer_id", "customerid"]),
        pick_first_col(orders, ["orgid", "org_id"]),
        pick_first_col(orders, ["orderdateutc", "order_date"]),
    ]

    join_cols = [c for c in join_cols if c]

    orders_small = orders[join_cols].drop_duplicates(order_id_o)

    fact = lines.merge(
        orders_small,
        left_on=order_id_l,
        right_on=order_id_o,
        how="left",
    )

    fact["gold_load_ts"] = now_utc_str()

    write_single_csv_to_s3(fact, GOLD_FACT_SALES)


if __name__ == "__main__":
    main()