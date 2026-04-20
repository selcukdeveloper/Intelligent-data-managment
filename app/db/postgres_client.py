import os
import psycopg2
from psycopg2.extras import RealDictCursor


class PostgresClient:
    def __init__(self):
        self.conn_params = {
            "host": os.environ.get("PG_HOST", "localhost"),
            "port": os.environ.get("PG_PORT", "5432"),
            "dbname": os.environ.get("PG_DB", "postgres"),
            "user": os.environ.get("PG_USER", "postgres"),
            "password": os.environ.get("PG_PASSWORD", ""),
            "sslmode": os.environ.get("PG_SSLMODE", "prefer"),
            "connect_timeout": 10,
        }

    def _get_conn(self):
        return psycopg2.connect(**self.conn_params)

    def execute(self, query: str):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                if cur.description is None:
                    conn.commit()
                    return [], []
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                rows = [{k: _jsonify(v) for k, v in row.items()} for row in rows]
                return columns, rows
        finally:
            conn.close()


def _jsonify(v):
    from decimal import Decimal
    from datetime import date, datetime
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v
