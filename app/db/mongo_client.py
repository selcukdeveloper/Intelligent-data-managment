import os
import json
import certifi
from pymongo import MongoClient as PyMongoClient


class MongoClient:
    def __init__(self):
        self.uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = os.environ.get("MONGO_DB", "ecommerce_dw")
        self._client = None

    def _get_db(self):
        if self._client is None:
            try:
                ca_bundle = certifi.where()
                self._client = PyMongoClient(
                    self.uri,
                    tlsCAFile=ca_bundle,
                    tlsAllowInvalidCertificates=True,
                    serverSelectionTimeoutMS=10000,
                    connectTimeoutMS=10000,
                    retryWrites=False
                )
                self._client.admin.command('ping')
            except Exception as e:
                raise ConnectionError(f"MongoDB connection failed: {str(e)}")
        return self._client[self.db_name]

    def execute(self, query: str):
        spec = json.loads(query)
        collection_name = spec["collection"]
        operation = spec.get("operation", "find")
        db = self._get_db()
        coll = db[collection_name]

        if operation == "aggregate":
            pipeline = spec.get("pipeline", [])
            cursor = coll.aggregate(pipeline)
            docs = list(cursor)
        elif operation == "find":
            filter_ = spec.get("filter", {})
            projection = spec.get("projection")
            limit = spec.get("limit", 100)
            cursor = coll.find(filter_, projection).limit(limit)
            docs = list(cursor)
        else:
            raise ValueError(f"Unsupported operation: {operation}")

        rows = []
        columns = []
        seen = set()
        for doc in docs:
            row = {}
            for k, v in doc.items():
                if k not in seen:
                    columns.append(k)
                    seen.add(k)
                row[k] = _serialize(v)
            rows.append(row)
        return columns, rows


def _serialize(v):
    from bson import ObjectId
    from datetime import date, datetime
    from decimal import Decimal
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, dict):
        return {k: _serialize(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_serialize(x) for x in v]
    return v