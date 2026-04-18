import os
from neo4j import GraphDatabase


class Neo4jClient:
    def __init__(self):
        self.uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.environ.get("NEO4J_USER", "neo4j")
        self.password = os.environ.get("NEO4J_PASSWORD", "neo4j")
        self._driver = None

    def _get_driver(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                self.uri, auth=(self.user, self.password)
            )
        return self._driver

    def execute(self, query: str):
        driver = self._get_driver()
        with driver.session() as session:
            result = session.run(query)
            records = list(result)
            if not records:
                return [], []
            columns = list(records[0].keys())
            rows = []
            for rec in records:
                row = {}
                for k in columns:
                    row[k] = _serialize(rec[k])
                rows.append(row)
            return columns, rows

    def close(self):
        if self._driver:
            self._driver.close()


def _serialize(v):
    if hasattr(v, "items") and not isinstance(v, dict):
        return dict(v.items())
    if isinstance(v, list):
        return [_serialize(x) for x in v]
    try:
        import json
        json.dumps(v)
        return v
    except (TypeError, ValueError):
        return str(v)