from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path

import certifi
from pymongo import MongoClient


MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _parse_date(raw: str) -> datetime | None:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None


def build_doc(row: dict[str, str], line_no: int) -> dict | None:
    order_date = _parse_date(row.get("Order Date", ""))
    if order_date is None:
        return None
    try:
        return {
            "sales_fact_id": f"{row['Order ID'].strip()}-{line_no}",
            "sales_amount":  float(row["Sales"]),
            "quantity":      int(row["Quantity"]),
            "profit_amount": float(row["Profit"]),
            "product": {
                "product_name": row["Product Name"].strip(),
                "category":     row["Category"].strip(),
            },
            "customer": {
                "customer_name": row["Customer Name"].strip(),
            },
            "date": {
                "day":        order_date.day,
                "month":      order_date.month,
                "month_name": MONTH_NAMES[order_date.month],
                "year":       order_date.year,
            },
            "location": {
                "state":  row["State"].strip(),
                "region": row["Region"].strip(),
            },
            "ship_mode": row["Ship Mode"].strip(),
        }
    except (KeyError, ValueError):
        return None


def load(csv_path: Path) -> None:
    client = MongoClient(
        os.environ["MONGO_URI"],
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=10000,
    )
    db = client[os.environ.get("MONGO_DB", "ecommerce_dw")]
    db.sales.drop()
    db.sales.create_index("product.category")
    db.sales.create_index("customer.customer_name")
    db.sales.create_index([("date.year", 1), ("date.month", 1)])

    docs: list[dict] = []
    with csv_path.open(newline="", encoding="latin-1") as fh:
        reader = csv.DictReader(fh)
        for i, row in enumerate(reader):
            doc = build_doc(row, i)
            if doc is not None:
                docs.append(doc)
            if len(docs) >= 1000:
                db.sales.insert_many(docs)
                docs.clear()
    if docs:
        db.sales.insert_many(docs)

    print(f"Loaded into {db.name}.sales  total: {db.sales.count_documents({})}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", type=Path, required=True)
    args = p.parse_args(argv)
    if not args.csv.is_file():
        print(f"CSV not found: {args.csv}", file=sys.stderr)
        return 2
    load(args.csv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
