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
