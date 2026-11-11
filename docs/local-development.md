# Local development

Requires Python 3.11+ and Docker Compose (or an existing PostgreSQL instance). Run commands from the repository root.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -c requirements-dev.lock -e '.[dev]'
cp .env.example .env
```

Set `ALERTOPS_WEBHOOK_SECRET` in `.env` to a random local secret. The demo simulator reads the same value. Do not commit `.env`.

```bash
docker compose up -d --wait postgres
alembic upgrade head
python -m app.seed
uvicorn app.main:create_app --factory --reload
```

In another terminal with the virtual environment activated:

```bash
python -m scripts.send_demo
```

Run the sender repeatedly: each successful request returns `202` with the same event and job IDs. Inspect the schema and endpoint documentation at `http://127.0.0.1:8000/docs`.

The current workflow persists an event, a queued job, and an unpublished outbox entry. No worker is running yet; fulfillment will not execute. `/health` is a process liveness check, not a database readiness check.

The next steps are described in the [fulfillment workflow design](fulfillment-workflow.md). The dispatcher and worker remain planned; instructions for running the implemented simulator are below.

## Tests

```bash
ruff check .
export ALERTOPS_TEST_DATABASE_URL='postgresql+psycopg://alertops:alertops@localhost:5432/alertops'
pytest -q
```

Each database test creates and removes a uniquely named test schema. Use a development/test database and credentials allowed to create schemas. Without the test URL, PostgreSQL integration tests are explicitly skipped. CI supplies a real PostgreSQL service and runs the full suite.

The initial migration is a fixed schema snapshot, independent of future ORM model changes. Tests also check for migration/model drift. Event update/delete operations are blocked by a database trigger. Local development uses the database owner; separate least-privilege runtime credentials are required before a hosted deployment.

## Current operational boundaries

- The demo secret resolver supports only `demo_webhook_secret`. Each integration has a reference; unsupported references are rejected. Multiple independently configured secrets and secret rotation will be implemented when needed.
- Request bodies are capped at 64 KiB, including streamed/chunked requests. The rate limit is a fixed 120 requests per minute per API process; deployment needs a shared limiter or ingress enforcement.
- Signatures authenticate the raw bytes. Deduplication hashes normalized validated JSON, so whitespace/key order and equivalent timezone representations do not conflict.
- Invalid requests and conflicts are logged with a generated request ID and status; payloads and database error parameters are excluded.
- Operator reads and tenant authorization endpoints are not exposed in this slice. Tenant ownership is persisted through the integration relationship.
- PostgreSQL must provide durable commits. Redis is not used by this workflow.

Stop local services with `docker compose stop`. The named database volume preserves local data.

## Fulfillment simulator and adapter

The persistent simulator and HTTP adapter are implemented. They are not connected to queued AlertOps jobs yet; the dispatcher and worker are the next slice.

Add these settings to your local `.env` (existing files are not updated automatically):

```dotenv
SIMULATOR_DATABASE_URL=postgresql+psycopg://simulator:simulator@localhost:5433/simulator
SIMULATOR_ACCOUNTS={"demo-store-01":"your-separate-provider-api-key"}
```

Use a different random credential from `ALERTOPS_WEBHOOK_SECRET`. Account names define stable namespaces; keep them unchanged across restarts. The simulator authenticates the key and derives the account server-side.

```bash
docker compose up -d --wait simulator-postgres
alembic -c simulator/alembic.ini upgrade head
uvicorn simulator.main:create_app --factory --host 127.0.0.1 --port 8001
```

The simulator uses a separate PostgreSQL container, database, and persistent volume to prevent accidental sharing of application tables. This adds local resource cost but makes the external-service boundary explicit. Its database is migrated independently; application migrations remain unchanged. Run commands from the repository root.

Open `http://127.0.0.1:8001/docs` to inspect the API. Authenticate with the provider key and send `POST /v1/shipments` with an `Idempotency-Key` header and `order_id`, `resource_version`, and `item_count` fields. The first request returns `201`; an identical repeat returns `200` with the same shipment ID. Read back through `GET /v1/shipments/by-operation?operation_key=...`. Reusing a key with different content, or a different key for an already represented order, returns `409`.

AlertOps code uses `app.providers.fulfillment.FulfillmentAdapter` as a context manager. Pass the provider URL and account credential explicitly. `create_shipment(Operation(...))` validates the creation response; `verify_shipment(operation)` independently reads back and checks every expected field. Neither method retries automatically. A creation timeout or invalid response raises `UnknownOutcome`; a failed lookup raises `LookupUnavailable`. `None` means a successful lookup found no shipment, not permission to repeat a create call.

The existing `pytest` command includes simulator tests. PostgreSQL tests run a real HTTP server on an ephemeral localhost port, verify restart persistence and concurrent idempotency, and compare the simulator migration to its models. Adapter error tests deliberately use controlled HTTP responses for deterministic timeout and malformed-response checks. CI runs these through its existing PostgreSQL service using isolated schemas.

No artificial fault-injection controls, worker, physical shipping, or real-provider integration are included yet. The simulator's normal read-back is immediately consistent.
