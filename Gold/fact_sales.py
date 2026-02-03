from __future__ import annotations

import awswrangler as wr

from common.s3_write import write_single_csv_to_s3
from common.utils import (
    now_utc_str,
    pick_first_col,
    standardize_columns,
    trim_strings,
    to_numeric_safe,
)

SILVER_ORDERS = "s3://XXXXXXX.csv"
SILVER_ORDERLINES = "s3://XXXXXcsv"
GOLD_FACT_SALES = "s3://sXXXXXX.csv"


def main():
    # --------------------
    # Read Silver
    # --------------------
    orders = wr.s3.read_csv(SILVER_ORDERS)
    lines = wr.s3.read_csv(SILVER_ORDERLINES)

    orders = trim_strings(standardize_columns(orders))
    lines = trim_strings(standardize_columns(lines))

    # --------------------
    # Normalize order_date column
    # --------------------
    if "orderdateutc" in orders.columns:
        orders = orders.rename(columns={"orderdateutc": "order_date"})

    # --------------------
    # Identify keys
    # --------------------
    order_id_o = pick_first_col(orders, ["order_id", "orderid"])
    order_id_l = pick_first_col(lines, ["order_id", "orderid"])
    if not order_id_o or not order_id_l:
        raise ValueError("Orders and OrderLines must contain order_id.")

    customer_key = pick_first_col(orders, ["customer_id", "customerid", "cust_id"])
    org_key = pick_first_col(orders, ["orgid", "org_id", "organizationid"])
    product_key = pick_first_col(lines, ["product_id", "productid", "sku", "item_id"])

    qty_col = pick_first_col(lines, ["quantity", "qty", "order_qty"])
    unit_price_col = pick_first_col(lines, ["unit_price", "unitprice", "price"])
    discount_col = pick_first_col(lines, ["discountamount", "discount", "discountamt"])

    # --------------------
    # Build sales_amount
    # --------------------
    if qty_col and unit_price_col:
        sales_amount = (
            to_numeric_safe(lines[qty_col]) *
            to_numeric_safe(lines[unit_price_col])
        )
    else:
        sales_amount = 0

    if discount_col:
        sales_amount = sales_amount - to_numeric_safe(lines[discount_col]).fillna(0)

    lines = lines.copy()
    lines["sales_amount"] = sales_amount.fillna(0) if hasattr(sales_amount, "fillna") else sales_amount

    # --------------------
    # Ensure line_id
    # --------------------
    line_id = pick_first_col(lines, ["order_line_id", "orderline_id", "line_id"])
    if not line_id:
        lines["line_id"] = lines.groupby(order_id_l).cumcount() + 1
        line_id = "line_id"

    # --------------------
    # Join Orders → Lines
    # --------------------
    join_cols = [order_id_o, "order_date"]
    if customer_key:
        join_cols.append(customer_key)
    if org_key:
        join_cols.append(org_key)

    orders_join = orders[join_cols].drop_duplicates(subset=[order_id_o])

    fact = lines.merge(
        orders_join,
        left_on=order_id_l,
        right_on=order_id_o,
        how="left",
        suffixes=("", "_order"),
    )

    # --------------------
    # Final Gold column order (STABLE)
    # --------------------
    fact = fact[
        [
            order_id_l,
            line_id,
            customer_key,
            product_key,
            org_key,
            qty_col,
            unit_price_col,
            discount_col,
            "sales_amount",
            "order_date",
        ]
    ].copy()

    fact["gold_load_ts"] = now_utc_str()

    write_single_csv_to_s3(fact, GOLD_FACT_SALES)


if __name__ == "__main__":
    main()
