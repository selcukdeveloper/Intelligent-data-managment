from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import Any

from .charts import CHART_PRESETS
from .mongo import QUERIES as MONGO_QUERIES
from .neo4j import QUERIES as NEO4J_QUERIES
from .postgres import QUERIES as POSTGRES_QUERIES


BACKENDS = ("postgres", "neo4j", "mongo")
_OVERRIDES_FILE = Path(__file__).parent / "overrides.json"
_lock = Lock()


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


_BUILTIN: dict[str, dict] = {
    "postgres": _attach_charts(POSTGRES_QUERIES),
    "neo4j":    _attach_charts(NEO4J_QUERIES),
    "mongo":    _attach_charts(MONGO_QUERIES),
}


def _empty_overrides() -> dict:
    return {"overrides": {}, "deleted_keys": []}


def _load_overrides() -> dict:
    if not _OVERRIDES_FILE.is_file():
        return _empty_overrides()
    try:
        data = json.loads(_OVERRIDES_FILE.read_text())
    except json.JSONDecodeError:
        return _empty_overrides()
    data.setdefault("overrides", {})
    data.setdefault("deleted_keys", [])
    return data


def _save_overrides(data: dict) -> None:
    _OVERRIDES_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def get_queries(db_name: str) -> dict:
    if db_name not in _BUILTIN:
        return {}
    with _lock:
        overrides = _load_overrides()
    merged = deepcopy(_BUILTIN[db_name])
    for key, spec in overrides["overrides"].items():
        if db_name not in spec:
            continue
        merged[key] = {
            "description": spec.get("description", merged.get(key, {}).get("description", key)),
            "query":       spec[db_name],
        }
        if key in _BUILTIN[db_name] and "chart" in _BUILTIN[db_name][key]:
            merged[key]["chart"] = _BUILTIN[db_name][key]["chart"]
    for key in overrides["deleted_keys"]:
        merged.pop(key, None)
    return merged


def get_all_keys() -> list[str]:
    keys: set[str] = set()
    for db in BACKENDS:
        keys.update(get_queries(db).keys())
    builtin_order = list(_BUILTIN["postgres"].keys())
    extras = sorted(k for k in keys if k not in builtin_order)
    return [k for k in builtin_order if k in keys] + extras


def get_all_queries_admin_view() -> list[dict]:
    result: list[dict] = []
    overrides = _load_overrides()
    for key in get_all_keys():
        record: dict[str, Any] = {
            "key": key,
            "description": "",
            "is_builtin": key in _BUILTIN["postgres"] or key in _BUILTIN["neo4j"] or key in _BUILTIN["mongo"],
            "has_override": key in overrides["overrides"],
        }
        for db in BACKENDS:
            qmap = get_queries(db)
            if key in qmap:
                record[db] = qmap[key].get("query", "")
                if not record["description"]:
                    record["description"] = qmap[key].get("description", "")
            else:
                record[db] = ""
        result.append(record)
    return result


def upsert_query(
    key: str,
    description: str,
    queries_by_db: dict[str, str],
) -> None:
    if not key:
        raise ValueError("query key is required")
    key = key.strip()
    description = (description or "").strip()

    cleaned = {db: (queries_by_db.get(db) or "").strip() for db in BACKENDS}
    if not any(cleaned.values()):
        raise ValueError("at least one backend must have a non-empty query")

    with _lock:
        data = _load_overrides()
        data["overrides"][key] = {"description": description, **cleaned}
        if key in data["deleted_keys"]:
            data["deleted_keys"] = [k for k in data["deleted_keys"] if k != key]
        _save_overrides(data)


def delete_query(key: str) -> None:
    with _lock:
        data = _load_overrides()
        data["overrides"].pop(key, None)
        is_builtin = any(key in _BUILTIN[db] for db in BACKENDS)
        if is_builtin and key not in data["deleted_keys"]:
            data["deleted_keys"].append(key)
        _save_overrides(data)


class _LiveQueries:
    def __getitem__(self, db_name: str) -> dict:
        return get_queries(db_name)

    def get(self, db_name: str, default: Any = None) -> Any:
        if db_name not in _BUILTIN:
            return default
        return get_queries(db_name)

    def keys(self):
        return BACKENDS


PREDEFINED_QUERIES = _LiveQueries()
