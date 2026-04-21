from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2


STAGING_DDL = """
DROP TABLE IF EXISTS stg_superstore;
CREATE TABLE stg_superstore (
    order_id       TEXT,
    order_date     DATE,
    ship_mode      TEXT,
    customer_name  TEXT,
    state          TEXT,
    region         TEXT,
    category       TEXT,
    product_name   TEXT,
    sales          NUMERIC,
    quantity       INT,
    profit         NUMERIC
);
"""


def _parse_date(raw: str) -> datetime | None:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None


def clean_row(row: dict[str, str]) -> tuple | None:
    order_date = _parse_date(row.get("Order Date", ""))
    if order_date is None:
        return None
    try:
        return (
            row["Order ID"].strip(),
            order_date.date(),
            row["Ship Mode"].strip(),
            row["Customer Name"].strip(),
            row["State"].strip(),
            row["Region"].strip(),
            row["Category"].strip(),
            row["Product Name"].strip(),
            float(row["Sales"]),
            int(row["Quantity"]),
            float(row["Profit"]),
        )
    except (KeyError, ValueError):
        return None


def _conn_params() -> dict:
    return {
        "host":     os.environ["PG_HOST"],
        "port":     os.environ.get("PG_PORT", "5432"),
        "dbname":   os.environ.get("PG_DB", "postgres"),
        "user":     os.environ.get("PG_USER", "postgres"),
        "password": os.environ["PG_PASSWORD"],
        "sslmode":  "require",
    }


def load(csv_path: Path) -> None:
    with psycopg2.connect(**_conn_params()) as conn, conn.cursor() as cur:
        cur.execute(STAGING_DDL)

        clean_rows = []
        with csv_path.open(newline="", encoding="latin-1") as fh:
            reader = csv.DictReader(fh)
            for raw in reader:
                row = clean_row(raw)
                if row is not None:
                    clean_rows.append(row)

        print(f"Loaded {len(clean_rows)} clean rows into staging.")

        insert_sql = """
            INSERT INTO stg_superstore (
                order_id, order_date, ship_mode, customer_name, state,
                region, category, product_name, sales, quantity, profit
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(insert_sql, clean_rows)

        script = Path(__file__).resolve().parent.parent / \
            "ddl" / "postgres" / "03_load_star_from_staging.sql"
        cur.execute(script.read_text())

        print("Dimensions + fact loaded.")


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
