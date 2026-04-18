from __future__ import annotations

from copy import deepcopy

from .charts import CHART_PRESETS
from .mongo import QUERIES as MONGO_QUERIES
from .neo4j import QUERIES as NEO4J_QUERIES
from .postgres import QUERIES as POSTGRES_QUERIES


def _attach_charts(db_queries: dict) -> dict:
    merged: dict = {}
    for query_key, spec in db_queries.items():
        spec_copy = deepcopy(spec)
        if "chart" not in spec_copy:
            preset = CHART_PRESETS.get(query_key)
            if preset is not None:
                spec_copy["chart"] = preset
        merged[query_key] = spec_copy
    return merged


PREDEFINED_QUERIES = {
    "postgres": _attach_charts(POSTGRES_QUERIES),
    "neo4j": _attach_charts(NEO4J_QUERIES),
    "mongo": _attach_charts(MONGO_QUERIES),
}
