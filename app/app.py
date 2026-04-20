"""
DW Umbrella Frontend — KT553 Project
Supports: PostgreSQL (Supabase), Neo4j, MongoDB
"""
import time
import uuid
from datetime import datetime, timezone
from flask import Flask, render_template, request, jsonify
from db.postgres_client import PostgresClient
from db.neo4j_client import Neo4jClient
from db.mongo_client import MongoClient
from queries.registry import PREDEFINED_QUERIES
from streaming.kafka_producer import QueryEventProducer

app = Flask(__name__)

# Initialize DB clients (lazy — they connect on first use)
clients = {
    "postgres": PostgresClient(),
    "neo4j": Neo4jClient(),
    "mongo": MongoClient(),
}
query_event_producer = QueryEventProducer()

DB_LABELS = {
    "postgres": "PostgreSQL (Supabase)",
    "neo4j": "Neo4j",
    "mongo": "MongoDB",
}


def _build_query_event(
    *,
    db_name: str,
    query_type: str,
    query_key: str | None,
    success: bool,
    row_count: int,
    elapsed_ms: float,
) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "db_name": db_name,
        "query_type": query_type,
        "query_key": query_key,
        "success": success,
        "row_count": row_count,
        "elapsed_ms": round(elapsed_ms, 2),
        "executed_at": datetime.now(timezone.utc).isoformat(),
    }


def _publish_query_event(event: dict) -> str | None:
    try:
        query_event_producer.publish(event)
        return None
    except RuntimeError as stream_err:
        app.logger.error("Kafka publish failed: %s", stream_err)
        return str(stream_err)


@app.route("/")
def index():
    """Umbrella landing page."""
    return render_template("index.html")


@app.route("/explorer/<db_name>")
def explorer(db_name):
    """Individual DB explorer page."""
    if db_name not in clients:
        return "Unknown database", 404
    queries = PREDEFINED_QUERIES.get(db_name, {})
    return render_template(
        "explorer.html",
        db_name=db_name,
        db_label=DB_LABELS[db_name],
        queries=queries,
    )


@app.route("/compare")
def compare():
    """Side-by-side comparison view."""
    # Use query keys from postgres as the canonical list (same semantic query across DBs)
    query_keys = list(PREDEFINED_QUERIES["postgres"].keys())
    return render_template("compare.html", query_keys=query_keys)


# ---------- API endpoints ----------

@app.route("/api/run", methods=["POST"])
def run_query():
    """Execute a query against one DB. Body: {db, query_type, query_key OR custom_query}"""
    data = request.get_json()
    db_name = data.get("db")
    query_type = data.get("query_type")  # "predefined" or "custom"
    
    if db_name not in clients:
        return jsonify({"error": f"Unknown DB: {db_name}"}), 400

    # Resolve the actual query string
    chart = None
    query_key = None
    if query_type == "predefined":
        query_key = data.get("query_key")
        query_info = PREDEFINED_QUERIES.get(db_name, {}).get(query_key)
        if not query_info:
            return jsonify({"error": "Unknown query key"}), 400
        query_str = query_info["query"]
        description = query_info.get("description", "")
        chart = query_info.get("chart")
    else:
        query_str = data.get("custom_query", "").strip()
        description = "Custom query"
        if not query_str:
            return jsonify({"error": "Empty query"}), 400

    # Execute and time it
    client = clients[db_name]
    start = time.perf_counter()
    try:
        columns, rows = client.execute(query_str)
        elapsed_ms = (time.perf_counter() - start) * 1000
        response = {
            "success": True,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "elapsed_ms": round(elapsed_ms, 2),
            "description": description,
            "query": query_str,
            "chart": chart,
        }
        streaming_error = _publish_query_event(
            _build_query_event(
                db_name=db_name,
                query_type=query_type,
                query_key=query_key,
                success=True,
                row_count=len(rows),
                elapsed_ms=elapsed_ms,
            )
        )
        if streaming_error is not None:
            response["streaming_error"] = streaming_error
        return jsonify(response)
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        response = {
            "success": False,
            "error": str(e),
            "elapsed_ms": round(elapsed_ms, 2),
            "query": query_str,
            "chart": chart,
        }
        streaming_error = _publish_query_event(
            _build_query_event(
                db_name=db_name,
                query_type=query_type,
                query_key=query_key,
                success=False,
                row_count=0,
                elapsed_ms=elapsed_ms,
            )
        )
        if streaming_error is not None:
            response["streaming_error"] = streaming_error
        return jsonify(response), 200  # 200 so the frontend can still render the error card


@app.route("/api/compare", methods=["POST"])
def run_compare():
    """Run the same semantic query on all three DBs."""
    data = request.get_json()
    query_key = data.get("query_key")
    results = {}
    streaming_errors = {}
    for db_name, client in clients.items():
        query_info = PREDEFINED_QUERIES.get(db_name, {}).get(query_key)
        if not query_info:
            results[db_name] = {"success": False, "error": "Query not defined for this DB"}
            continue
        start = time.perf_counter()
        try:
            columns, rows = client.execute(query_info["query"])
            elapsed_ms = (time.perf_counter() - start) * 1000
            results[db_name] = {
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "elapsed_ms": round(elapsed_ms, 2),
                "query": query_info["query"],
            }
            streaming_error = _publish_query_event(
                _build_query_event(
                    db_name=db_name,
                    query_type="compare_predefined",
                    query_key=query_key,
                    success=True,
                    row_count=len(rows),
                    elapsed_ms=elapsed_ms,
                )
            )
            if streaming_error is not None:
                streaming_errors[db_name] = streaming_error
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            results[db_name] = {
                "success": False,
                "error": str(e),
                "elapsed_ms": round(elapsed_ms, 2),
                "query": query_info["query"],
            }
            streaming_error = _publish_query_event(
                _build_query_event(
                    db_name=db_name,
                    query_type="compare_predefined",
                    query_key=query_key,
                    success=False,
                    row_count=0,
                    elapsed_ms=elapsed_ms,
                )
            )
            if streaming_error is not None:
                streaming_errors[db_name] = streaming_error
    if streaming_errors:
        results["_streaming_errors"] = streaming_errors
    return jsonify(results)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
