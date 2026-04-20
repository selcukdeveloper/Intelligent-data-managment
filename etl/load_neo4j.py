"""
Load the Kaggle "Superstore" CSV into Neo4j as a small graph that mirrors
the star schema.

Usage:
    python etl/load_neo4j.py --csv dataset.csv

Environment variables:
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

Graph model (see Methods §Neo4j Graph Design):

    (:Sale {sales_fact_id, sales_amount, quantity, profit_amount})
      -[:OF_PRODUCT]->      (:Product)-[:IN_CATEGORY]->(:Category)
      -[:BY_CUSTOMER]->     (:Customer)
      -[:SHIPPED_TO]->      (:State {region})
      -[:ON_DATE]->         (:Date)-[:IN_MONTH]->(:Month {year, month,
                                                          month_name})
      -[:USES_SHIP_MODE]->  (:ShipMode)

"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path

from neo4j import GraphDatabase


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


CREATE_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product)  REQUIRE p.product_name   IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category) REQUIRE c.category_name  IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (u:Customer) REQUIRE u.customer_name  IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (s:State)    REQUIRE s.state_name     IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Month)    REQUIRE (m.year, m.month) IS NODE KEY",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Date)     REQUIRE d.date           IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (h:ShipMode) REQUIRE h.ship_mode_name IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (x:Sale)     REQUIRE x.sales_fact_id  IS UNIQUE",
]

UPSERT_BATCH = """
UNWIND $rows AS row
MERGE (p:Product  {product_name: row.product_name})
MERGE (cat:Category {category_name: row.category})
MERGE (p)-[:IN_CATEGORY]->(cat)

MERGE (u:Customer {customer_name: row.customer_name})
MERGE (st:State   {state_name:    row.state})
  ON CREATE SET st.region = row.region
MERGE (sm:ShipMode {ship_mode_name: row.ship_mode})
MERGE (d:Date {date: row.date})
MERGE (m:Month {year: row.year, month: row.month})
  ON CREATE SET m.month_name = row.month_name
MERGE (d)-[:IN_MONTH]->(m)

MERGE (s:Sale {sales_fact_id: row.sales_fact_id})
  ON CREATE SET s.sales_amount  = row.sales,
                s.quantity      = row.quantity,
                s.profit_amount = row.profit
MERGE (s)-[:OF_PRODUCT]->(p)
MERGE (s)-[:BY_CUSTOMER]->(u)
MERGE (s)-[:SHIPPED_TO]->(st)
MERGE (s)-[:ON_DATE]->(d)
MERGE (s)-[:USES_SHIP_MODE]->(sm);
"""


def row_to_params(row: dict[str, str], line_no: int) -> dict | None:
    order_date = _parse_date(row.get("Order Date", ""))
    if order_date is None:
        return None
    try:
        return {
            "sales_fact_id": f"{row['Order ID'].strip()}-{line_no}",
            "product_name":  row["Product Name"].strip(),
            "category":      row["Category"].strip(),
            "customer_name": row["Customer Name"].strip(),
            "state":         row["State"].strip(),
            "region":        row["Region"].strip(),
            "ship_mode":     row["Ship Mode"].strip(),
            "date":          order_date.date().isoformat(),
            "year":          order_date.year,
            "month":         order_date.month,
            "month_name":    MONTH_NAMES[order_date.month],
            "sales":         float(row["Sales"]),
            "quantity":      int(row["Quantity"]),
            "profit":        float(row["Profit"]),
        }
    except (KeyError, ValueError):
        return None


def load(csv_path: Path) -> None:
    driver = GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
    )
    try:
        with driver.session() as session:
            for stmt in CREATE_CONSTRAINTS:
                session.run(stmt)

            batch: list[dict] = []
            total = 0
            with csv_path.open(newline="", encoding="latin-1") as fh:
                reader = csv.DictReader(fh)
                for i, row in enumerate(reader):
                    params = row_to_params(row, i)
                    if params is None:
                        continue
                    batch.append(params)
                    if len(batch) >= 500:
                        session.run(UPSERT_BATCH, rows=batch)
                        total += len(batch)
                        batch.clear()
            if batch:
                session.run(UPSERT_BATCH, rows=batch)
                total += len(batch)

            print(f"Loaded {total} Sale nodes (plus dimensions) into Neo4j.")
    finally:
        driver.close()


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
