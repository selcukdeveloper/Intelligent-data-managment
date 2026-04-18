"""
DW Umbrella Frontend — KT553 Project
Supports: PostgreSQL (Supabase), Neo4j, MongoDB
"""
import time
from flask import Flask, render_template, request, jsonify
from db.postgres_client import PostgresClient
from db.neo4j_client import Neo4jClient
from db.mongo_client import MongoClient
from queries.registry import PREDEFINED_QUERIES

app = Flask(__name__)

# Initialize DB clients (lazy — they connect on first use)
clients = {
    "postgres": PostgresClient(),
    "neo4j": Neo4jClient(),
    "mongo": MongoClient(),
}

DB_LABELS = {
    "postgres": "PostgreSQL (Supabase)",
    "neo4j": "Neo4j",
    "mongo": "MongoDB",
}


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
        return jsonify({
            "success": True,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "elapsed_ms": round(elapsed_ms, 2),
            "description": description,
            "query": query_str,
            "chart": chart,
        })
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return jsonify({
            "success": False,
            "error": str(e),
            "elapsed_ms": round(elapsed_ms, 2),
            "query": query_str,
            "chart": chart,
        }), 200  # 200 so the frontend can still render the error card


@app.route("/api/compare", methods=["POST"])
def run_compare():
    """Run the same semantic query on all three DBs."""
    data = request.get_json()
    query_key = data.get("query_key")
    results = {}
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
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            results[db_name] = {
                "success": False,
                "error": str(e),
                "elapsed_ms": round(elapsed_ms, 2),
                "query": query_info["query"],
            }
    return jsonify(results)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
