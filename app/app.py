import csv
import io
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any
from flask import Flask, render_template, request, jsonify, Response, redirect, url_for, session

from db.postgres_client import PostgresClient
from db.neo4j_client import Neo4jClient
from db.mongo_client import MongoClient
from queries.registry import (
    PREDEFINED_QUERIES,
    get_all_queries_admin_view,
    upsert_query,
    delete_query as delete_query_spec,
)
from streaming.kafka_producer import QueryEventProducer
from streaming.ingest_producer import row_to_event
from streaming.fanout_consumer import PostgresSink, MongoSink, Neo4jSink
from auth import (
    check_credentials,
    current_user,
    login_required,
    role_required,
    visible_query_keys,
    can_run_custom,
    can_ingest,
    can_edit_queries,
)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-only-change-me-in-prod")


@app.context_processor
def _inject_user():
    user = current_user()
    role = user["role"] if user else None
    return {
        "current_user":    user,
        "can_ingest":      can_ingest(role),
        "can_run_custom":  can_run_custom(role),
        "can_edit_queries": can_edit_queries(role),
    }


def _filter_queries_for_role(db_queries: dict, role: str | None) -> dict:
    allowed = visible_query_keys(role)
    if allowed is None:
        return db_queries
    return {k: v for k, v in db_queries.items() if k in allowed}

clients = {
    "postgres": PostgresClient(),
    "neo4j":    Neo4jClient(),
    "mongo":    MongoClient(),
}
query_event_producer = QueryEventProducer()

DB_LABELS = {
    "postgres": "PostgreSQL (Supabase)",
    "neo4j":    "Neo4j",
    "mongo":    "MongoDB",
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
        "event_id":    str(uuid.uuid4()),
        "db_name":     db_name,
        "query_type":  query_type,
        "query_key":   query_key,
        "success":     success,
        "row_count":   row_count,
        "elapsed_ms":  round(elapsed_ms, 2),
        "executed_at": datetime.now(timezone.utc).isoformat(),
    }


def _publish_query_event(event: dict) -> str | None:
    try:
        query_event_producer.publish(event)
        return None
    except RuntimeError as stream_err:
        app.logger.error("Kafka publish failed: %s", stream_err)
        return str(stream_err)


@app.route("/login", methods=["GET", "POST"])
def login():
    next_url = request.values.get("next") or url_for("index")
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        role = check_credentials(username, password)
        if role is None:
            return render_template(
                "login.html",
                error="Invalid username or password.",
                next_url=next_url,
            ), 401
        session.clear()
        session["username"] = username
        session["role"] = role
        return redirect(next_url)
    return render_template("login.html", error=None, next_url=next_url)


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.before_request
def _require_session():
    public_endpoints = {"login", "static"}
    if request.endpoint in public_endpoints:
        return None
    if current_user() is None:
        return redirect(url_for("login", next=request.path))
    return None


@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/explorer/<db_name>")
@login_required
def explorer(db_name):
    if db_name not in clients:
        return "Unknown database", 404
    user = current_user()
    queries = _filter_queries_for_role(
        PREDEFINED_QUERIES.get(db_name, {}),
        user["role"],
    )
    return render_template(
        "explorer.html",
        db_name=db_name,
        db_label=DB_LABELS[db_name],
        queries=queries,
    )


@app.route("/compare")
@login_required
def compare():
    user = current_user()
    all_keys = list(PREDEFINED_QUERIES["postgres"].keys())
    allowed = visible_query_keys(user["role"])
    query_keys = all_keys if allowed is None else [k for k in all_keys if k in allowed]
    return render_template("compare.html", query_keys=query_keys)


@app.route("/ingest")
@role_required("admin")
def ingest_page():
    return render_template("ingest.html")


@app.route("/api/run", methods=["POST"])
@login_required
def run_query():
    user = current_user()
    role = user["role"]
    data = request.get_json()
    db_name    = data.get("db")
    query_type = data.get("query_type")

    if db_name not in clients:
        return jsonify({"error": f"Unknown DB: {db_name}"}), 400

    chart = None
    query_key = None
    if query_type == "predefined":
        query_key = data.get("query_key")
        allowed = visible_query_keys(role)
        if allowed is not None and query_key not in allowed:
            return jsonify({"error": "This query is not available for your role."}), 403
        query_info = PREDEFINED_QUERIES.get(db_name, {}).get(query_key)
        if not query_info:
            return jsonify({"error": "Unknown query key"}), 400
        query_str  = query_info["query"]
        description = query_info.get("description", "")
        chart = query_info.get("chart")
    else:
        if not can_run_custom(role):
            return jsonify({"error": "Custom queries are not available for your role."}), 403
        query_str = data.get("custom_query", "").strip()
        description = "Custom query"
        if not query_str:
            return jsonify({"error": "Empty query"}), 400

    client = clients[db_name]
    start = time.perf_counter()
    try:
        columns, rows = client.execute(query_str)
        elapsed_ms = (time.perf_counter() - start) * 1000
        response = {
            "success":     True,
            "columns":     columns,
            "rows":        rows,
            "row_count":   len(rows),
            "elapsed_ms":  round(elapsed_ms, 2),
            "description": description,
            "query":       query_str,
            "chart":       chart,
        }
        streaming_error = _publish_query_event(_build_query_event(
            db_name=db_name, query_type=query_type, query_key=query_key,
            success=True, row_count=len(rows), elapsed_ms=elapsed_ms,
        ))
        if streaming_error is not None:
            response["streaming_error"] = streaming_error
        return jsonify(response)
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        response = {
            "success":    False,
            "error":      str(e),
            "elapsed_ms": round(elapsed_ms, 2),
            "query":      query_str,
            "chart":      chart,
        }
        streaming_error = _publish_query_event(_build_query_event(
            db_name=db_name, query_type=query_type, query_key=query_key,
            success=False, row_count=0, elapsed_ms=elapsed_ms,
        ))
        if streaming_error is not None:
            response["streaming_error"] = streaming_error
        return jsonify(response), 200


@app.route("/api/compare", methods=["POST"])
@login_required
def run_compare():
    user = current_user()
    data = request.get_json()
    query_key = data.get("query_key")
    allowed = visible_query_keys(user["role"])
    if allowed is not None and query_key not in allowed:
        return jsonify({"error": "This query is not available for your role."}), 403
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
                "success":    True,
                "columns":    columns,
                "rows":       rows,
                "row_count":  len(rows),
                "elapsed_ms": round(elapsed_ms, 2),
                "query":      query_info["query"],
            }
            err = _publish_query_event(_build_query_event(
                db_name=db_name, query_type="compare_predefined",
                query_key=query_key, success=True,
                row_count=len(rows), elapsed_ms=elapsed_ms,
            ))
            if err:
                streaming_errors[db_name] = err
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            results[db_name] = {
                "success":    False,
                "error":      str(e),
                "elapsed_ms": round(elapsed_ms, 2),
                "query":      query_info["query"],
            }
            err = _publish_query_event(_build_query_event(
                db_name=db_name, query_type="compare_predefined",
                query_key=query_key, success=False,
                row_count=0, elapsed_ms=elapsed_ms,
            ))
            if err:
                streaming_errors[db_name] = err
    if streaming_errors:
        results["_streaming_errors"] = streaming_errors
    return jsonify(results)


@app.route("/api/ingest/preview", methods=["POST"])
@role_required("admin")
def api_ingest_preview():
    upload = request.files.get("csv")
    if upload is None or upload.filename == "":
        return jsonify({"error": "No CSV file uploaded"}), 400

    raw = upload.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")

    events: list[dict] = []
    skipped: list[dict] = []
    reader = csv.DictReader(io.StringIO(text))
    for i, row in enumerate(reader):
        event = row_to_event(row, i)
        if event is None:
            skipped.append({
                "line": i + 2,
                "reason": "invalid row (missing/bad fields)",
            })
            continue
        events.append(event)

    return jsonify({
        "filename": upload.filename,
        "events":   events,
        "skipped":  skipped,
    })


@app.route("/api/ingest/commit", methods=["POST"])
@role_required("admin")
def api_ingest_commit():
    body = request.get_json(silent=True) or {}
    events = body.get("events")
    if not isinstance(events, list) or not events:
        return jsonify({"error": "Request body must be {\"events\": [...]}"}), 400

    def stream():
        try:
            sinks = {
                "postgres": PostgresSink(),
                "mongo":    MongoSink(),
                "neo4j":    Neo4jSink(),
            }
        except Exception as exc:
            yield json.dumps({
                "type": "error",
                "message": f"Could not initialise backends: {exc}",
            }) + "\n"
            return

        processed = 0
        try:
            for event in events:
                if not isinstance(event, dict) or "sales_fact_id" not in event:
                    continue
                outcomes: dict[str, str] = {}
                for name, sink in sinks.items():
                    try:
                        sink.write(event)
                        outcomes[name] = "ok"
                    except Exception as exc:
                        outcomes[name] = f"{exc.__class__.__name__}: {exc}"

                processed += 1
                yield json.dumps({
                    "type":          "row",
                    "sales_fact_id": event.get("sales_fact_id"),
                    "customer_name": event.get("customer_name"),
                    "sales_amount":  event.get("sales_amount"),
                    "postgres":      outcomes["postgres"],
                    "mongo":         outcomes["mongo"],
                    "neo4j":         outcomes["neo4j"],
                }) + "\n"
        finally:
            try:
                sinks["neo4j"].close()
            except Exception:
                pass
            yield json.dumps({
                "type":      "done",
                "processed": processed,
            }) + "\n"

    return Response(stream(), mimetype="application/x-ndjson")


@app.route("/admin/queries")
@role_required("admin")
def admin_queries_page():
    return render_template(
        "admin_queries.html",
        queries=get_all_queries_admin_view(),
    )


@app.route("/api/admin/queries", methods=["GET"])
@role_required("admin")
def api_admin_queries_list():
    return jsonify({"queries": get_all_queries_admin_view()})


@app.route("/api/admin/queries", methods=["POST"])
@role_required("admin")
def api_admin_queries_save():
    body = request.get_json(silent=True) or {}
    key = (body.get("key") or "").strip()
    if not key:
        return jsonify({"error": "query key is required"}), 400
    try:
        upsert_query(
            key=key,
            description=body.get("description", ""),
            queries_by_db={
                "postgres": body.get("postgres", ""),
                "neo4j":    body.get("neo4j", ""),
                "mongo":    body.get("mongo", ""),
            },
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"success": True, "key": key})


@app.route("/api/admin/queries/<path:key>", methods=["DELETE"])
@role_required("admin")
def api_admin_queries_delete(key: str):
    delete_query_spec(key)
    return jsonify({"success": True, "key": key})


@app.route("/admin/customers")
@role_required("admin")
def admin_customers_page():
    return render_template("admin_customers.html")


@app.route("/api/admin/customers/search", methods=["GET"])
@role_required("admin")
def api_admin_customers_search():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"customers": []})

    sql = """
        SELECT c.customer_name,
               COUNT(f.sales_fact_id)         AS order_count,
               COALESCE(SUM(f.sales_amount), 0)  AS total_sales,
               COALESCE(SUM(f.profit_amount), 0) AS total_profit
        FROM dim_customer c
        LEFT JOIN fact_sales f ON f.customer_key = c.customer_key
        WHERE c.customer_name ILIKE %s
        GROUP BY c.customer_name
        ORDER BY order_count DESC, c.customer_name ASC
        LIMIT 50
    """
    try:
        import psycopg2
        conn = psycopg2.connect(**clients["postgres"].conn_params)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (f"%{q}%",))
                rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as exc:
        return jsonify({"error": f"Postgres search failed: {exc}"}), 500

    customers = [
        {
            "customer_name": r[0],
            "order_count":   int(r[1] or 0),
            "total_sales":   float(r[2] or 0),
            "total_profit":  float(r[3] or 0),
        }
        for r in rows
    ]
    return jsonify({"customers": customers})


@app.route("/api/admin/customers/<path:customer_name>/transactions", methods=["GET"])
@role_required("admin")
def api_admin_customer_transactions(customer_name: str):
    try:
        db = clients["mongo"]._get_db()
        docs = list(
            db.sales.find(
                {"customer.customer_name": customer_name},
                {"_id": 0, "sales_fact_id": 1, "sales_amount": 1, "quantity": 1,
                 "profit_amount": 1, "product.product_name": 1,
                 "product.category": 1, "date": 1, "location.state": 1,
                 "ship_mode": 1},
            ).limit(500)
        )
    except Exception as exc:
        return jsonify({"error": f"Mongo lookup failed: {exc}"}), 500

    transactions = []
    for d in docs:
        transactions.append({
            "sales_fact_id": d.get("sales_fact_id"),
            "product_name":  (d.get("product") or {}).get("product_name"),
            "category":      (d.get("product") or {}).get("category"),
            "date":          _format_mongo_date(d.get("date")),
            "state":         (d.get("location") or {}).get("state"),
            "ship_mode":     d.get("ship_mode"),
            "sales_amount":  d.get("sales_amount"),
            "quantity":      d.get("quantity"),
            "profit_amount": d.get("profit_amount"),
        })
    return jsonify({"transactions": transactions})


def _format_mongo_date(d) -> str | None:
    if not isinstance(d, dict):
        return None
    y, m, day = d.get("year"), d.get("month"), d.get("day")
    if y and m and day:
        return f"{y:04d}-{m:02d}-{day:02d}"
    return None


@app.route("/api/admin/customers/<path:customer_name>", methods=["DELETE"])
@role_required("admin")
def api_admin_delete_customer(customer_name: str):
    outcomes: dict[str, dict] = {}

    try:
        import psycopg2
        conn = psycopg2.connect(**clients["postgres"].conn_params)
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT customer_key FROM dim_customer WHERE customer_name = %s",
                (customer_name,),
            )
            row = cur.fetchone()
            if row is None:
                outcomes["postgres"] = {"deleted_facts": 0, "deleted_customer": False}
            else:
                customer_key = row[0]
                cur.execute("DELETE FROM fact_sales WHERE customer_key = %s", (customer_key,))
                fact_deleted = cur.rowcount
                cur.execute("DELETE FROM dim_customer WHERE customer_key = %s", (customer_key,))
                outcomes["postgres"] = {
                    "deleted_facts":    fact_deleted,
                    "deleted_customer": cur.rowcount > 0,
                }
        conn.close()
    except Exception as exc:
        outcomes["postgres"] = {"error": str(exc)}

    try:
        db = clients["mongo"]._get_db()
        mongo_res = db.sales.delete_many({"customer.customer_name": customer_name})
        outcomes["mongo"] = {"deleted_count": mongo_res.deleted_count}
    except Exception as exc:
        outcomes["mongo"] = {"error": str(exc)}

    try:
        driver = clients["neo4j"]._get_driver()
        with driver.session() as sess:
            summary = sess.run(
                """
                MATCH (u:Customer {customer_name: $name})
                OPTIONAL MATCH (s:Sale)-[:BY_CUSTOMER]->(u)
                WITH u, collect(s) AS sales
                FOREACH (x IN sales | DETACH DELETE x)
                DETACH DELETE u
                RETURN size(sales) AS deleted_sales
                """,
                name=customer_name,
            ).single()
            outcomes["neo4j"] = {
                "deleted_sales": (summary["deleted_sales"] if summary else 0),
            }
    except Exception as exc:
        outcomes["neo4j"] = {"error": str(exc)}

    return jsonify({"customer_name": customer_name, "results": outcomes})


def _parse_sales_fact_id(sales_fact_id: str) -> tuple[str, int] | None:
    if not isinstance(sales_fact_id, str) or "-" not in sales_fact_id:
        return None
    order_id, sep, tail = sales_fact_id.rpartition("-")
    if not sep or not order_id or not tail:
        return None
    try:
        return order_id, int(tail)
    except ValueError:
        return None


def _customer_summary_pg(customer_name: str) -> dict | None:
    sql = """
        SELECT COUNT(f.sales_fact_id)         AS order_count,
               COALESCE(SUM(f.sales_amount), 0)  AS total_sales,
               COALESCE(SUM(f.profit_amount), 0) AS total_profit
        FROM dim_customer c
        LEFT JOIN fact_sales f ON f.customer_key = c.customer_key
        WHERE c.customer_name = %s
        GROUP BY c.customer_key
    """
    try:
        import psycopg2
        conn = psycopg2.connect(**clients["postgres"].conn_params)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (customer_name,))
                row = cur.fetchone()
        finally:
            conn.close()
    except Exception:
        return None
    if row is None:
        return {"order_count": 0, "total_sales": 0.0, "total_profit": 0.0}
    return {
        "order_count":  int(row[0] or 0),
        "total_sales":  float(row[1] or 0),
        "total_profit": float(row[2] or 0),
    }


@app.route("/api/admin/transactions", methods=["DELETE"])
@role_required("admin")
def api_admin_delete_transactions():
    body = request.get_json(silent=True) or {}
    ids = body.get("sales_fact_ids") or []
    customer_name = (body.get("customer_name") or "").strip() or None
    if not isinstance(ids, list) or not ids:
        return jsonify({"error": "body must be {sales_fact_ids: [...]}"}), 400
    ids = [str(x) for x in ids if x]

    outcomes: dict[str, Any] = {}

    parsed: list[tuple[str, int]] = []
    unparseable: list[str] = []
    for sid in ids:
        parts = _parse_sales_fact_id(sid)
        if parts is None:
            unparseable.append(sid)
        else:
            parsed.append(parts)

    try:
        import psycopg2
        conn = psycopg2.connect(**clients["postgres"].conn_params)
        pg_deleted = 0
        with conn, conn.cursor() as cur:
            for order_id, line_no in parsed:
                cur.execute(
                    "DELETE FROM fact_sales WHERE order_id = %s AND line_no = %s",
                    (order_id, line_no),
                )
                pg_deleted += cur.rowcount
        conn.close()
        outcomes["postgres"] = {
            "deleted_count": pg_deleted,
            "requested":     len(parsed),
            "unparseable":   unparseable,
        }
    except Exception as exc:
        outcomes["postgres"] = {"error": str(exc)}

    try:
        db = clients["mongo"]._get_db()
        res = db.sales.delete_many({"sales_fact_id": {"$in": ids}})
        outcomes["mongo"] = {"deleted_count": res.deleted_count}
    except Exception as exc:
        outcomes["mongo"] = {"error": str(exc)}

    try:
        driver = clients["neo4j"]._get_driver()
        with driver.session() as sess:
            summary = sess.run(
                """
                UNWIND $ids AS id
                MATCH (s:Sale {sales_fact_id: id})
                DETACH DELETE s
                RETURN count(s) AS deleted
                """,
                ids=ids,
            ).single()
            outcomes["neo4j"] = {
                "deleted_count": (summary["deleted"] if summary else 0),
            }
    except Exception as exc:
        outcomes["neo4j"] = {"error": str(exc)}

    response: dict[str, Any] = {"results": outcomes, "requested_ids": ids}
    if customer_name:
        summary = _customer_summary_pg(customer_name)
        if summary is not None:
            response["customer_summary"] = summary
            response["customer_name"] = customer_name
    return jsonify(response)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
