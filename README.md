# DW Umbrella Frontend

This is a small Flask web UI that can run the same “analytics” questions against
three different databases:

- PostgreSQL (Supabase)
- Neo4j (Aura)
- MongoDB (Atlas)

You can run queries on one database at a time, or run the “compare” view to see
results + timing side-by-side.

The app does not start the databases for you. It assumes you already have them
running somewhere and you just want a simple umbrella UI to query them.

## How to Start

You’ll need Docker Desktop (or Docker Engine) with Docker Compose.

1) Create your local env file:

```bash
cp .env .env
```

Open `.env` and fill in your connection info:

- Check the .env.example for more info

2) Build and run the app:

```bash
docker compose up --build
```

3) Open the UI:

http://localhost:5000

To stop it, press Ctrl+C. To remove the container:

```bash
docker compose down
```

## Where the predefined queries live

The dropdown “predefined” queries are just files in this repo:

- `app/queries/postgres.py` (SQL)
- `app/queries/neo4j.py` (Cypher)
- `app/queries/mongo.py` (Mongo JSON spec)

The compare page works by using the same query key across all three files.
So if you add a new query, add it to all of them (or the compare view will show
“Query not defined” for the missing databases).

## MongoDB custom query format

Mongo doesn’t have one query string language like SQL or Cypher, so the Mongo
custom query box expects JSON.

Example:

```json
{
   "collection": "sales_fact",
   "operation": "aggregate",
   "pipeline": [
      {"$group": {"_id": "$product.category", "revenue": {"$sum": "$total_amount"}}},
      {"$sort": {"revenue": -1}}
   ]
}
```

Supported `operation` values are `"find"` and `"aggregate"`.

## Small notes

- `docker-compose.yml` mounts `./app` into the container so you can edit and refresh quickly.
- Postgres connections use `sslmode=require` (good for Supabase). If you connect to a local Postgres, you may need to adjust it in `app/db/postgres_client.py`.
- Whitelist your IP adress in MongoDB database in order to access it