# Dispatcher and worker: how execution works

The webhook has already saved a job. The dispatcher is the messenger: it reads an outbox entry and places the job's ID on Redis. Celery runs the worker: it takes that ID, loads the saved order, asks the provider for a shipment, and reads the shipment back before declaring success.

PostgreSQL is the record of what actually happened. Redis is the delivery mechanism; losing or repeating a message must not lose or repeat the business effect.

## One order, step by step

1. The webhook commits the event, queued job, and outbox entry together and returns `202`.
2. The dispatcher locks one unpublished outbox entry, sends its job ID to Redis, then records publication. A failed publish leaves the entry available for a later poll.
3. The worker locks the job briefly and gives itself a claim token and a 60-second lease. It records an attempt and commits, releasing the lock before any network request.
4. The worker uses the integration's configured provider key and the job's stable operation key to create a shipment.
5. It independently reads back the provider shipment and matches the order ID, version, item count, operation key, and created state.
6. Only the worker holding the current unexpired claim can record success. The job stores the shipment ID and verification time; its attempt stores the outcome.

`SUCCEEDED` means a matching shipment record was verified. It does not mean a physical package shipped.

## States and safeguards

| State | Meaning |
| --- | --- |
| `QUEUED` | Durably accepted, waiting for a worker |
| `RUNNING` | A worker holds a temporary claim; its attempt is recorded |
| `SUCCEEDED` | Read-back confirmed the expected shipment |
| `UNKNOWN_OUTCOME` | The remote effect cannot currently be confirmed; verification only |
| `FAILED` | A definite rejection or invalid configuration stopped creation |

Repeated queue delivery for a running or succeeded job does not create another attempt or shipment. Provider idempotency supplies an additional safeguard if a request reaches the provider more than once.

Every two seconds, the dispatcher also scans for recovery work. An expired running lease becomes `UNKNOWN_OUTCOME`, and its unfinished attempt is marked `INTERRUPTED`. A subsequent attempt can only read the provider state. It never blindly creates another shipment. Old queued work whose message may have been lost becomes eligible for publication again.

An unknown outcome is checked again after 30 seconds. If the shipment is still absent or unreadable, it remains unknown. These repeated checks are read-only; automatic creation retries and backoff are intentionally deferred to the next reliability slice. In particular, a `429` is currently recorded as `FAILED`, not automatically retried. Incidents and approved recovery actions are not implemented yet.

The dispatcher may publish the same ID after a crash between publication and its database commit. A worker may die after the provider committed. These gaps are expected: guarded claims, the stable operation key, read-back, and recovery scans handle them together.

Celery uses late acknowledgments and worker-loss redelivery, JSON messages, one prefetched task per worker slot, and a 120-second Redis visibility timeout. PostgreSQL recovery is still required; these queue settings are not an exactly-once guarantee. See the official [Celery acknowledgment settings](https://docs.celeryq.dev/en/v5.6.3/userguide/configuration.html) and [Redis delivery caveats](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html).

## Configuration

Add these to the existing local `.env`:

```dotenv
ALERTOPS_BROKER_URL=redis://127.0.0.1:6379/0
ALERTOPS_PROVIDER_URL=http://127.0.0.1:8001
ALERTOPS_PROVIDER_KEYS={"demo-store-01":"your-provider-key"}
```

The key for `demo-store-01` must equal that account's credential in `SIMULATOR_ACCOUNTS`. It is separate from `ALERTOPS_WEBHOOK_SECRET`. Neither the queue message nor the database attempt contains the credential.

Optional settings are `ALERTOPS_LEASE_SECONDS` (default 60, minimum 15), `ALERTOPS_RECOVERY_SECONDS` (default 30), and `ALERTOPS_VERIFICATION_SECONDS` (default 30). Leave these defaults for the first demo.

## Run locally

From the repository root, activate Python and update dependencies:

```bash
source .venv/bin/activate
python -m pip install -c requirements-dev.lock -e '.[dev]'
docker compose up -d --wait postgres simulator-postgres redis
python -m alembic upgrade head
python -m alembic -c simulator/alembic.ini upgrade head
python -m app.seed
```

The application migration should report revision `0002` when you run `python -m alembic current`. It adds execution fields and attempt history without changing the accepted events. Its downgrade refuses to discard execution history silently.

Use separate terminal tabs for these four long-running commands. Activate the same environment and run from the repository root in each tab:

```bash
# Simulator HTTP service
python -m uvicorn simulator.main:create_app --factory --host 127.0.0.1 --port 8001

# AlertOps webhook API
python -m uvicorn app.main:create_app --factory --host 127.0.0.1 --port 8000

# Worker; solo pool is convenient for the local demo
python -m celery -A app.task_queue:celery_app worker --pool=solo --concurrency=1 --loglevel=INFO

# Dispatcher and recovery loop
python -m app.dispatcher
```

In a fifth terminal, send the demo event:

```bash
python -m scripts.send_demo
```

The `202` response still means accepted, not completed. After a few seconds, inspect the job and attempt:

```bash
docker compose exec postgres psql -U alertops -d alertops -c \
  "SELECT id, state, attempt_count, provider_shipment_id, verified_at FROM jobs ORDER BY created_at DESC LIMIT 5;"
docker compose exec postgres psql -U alertops -d alertops -c \
  "SELECT job_id, attempt_number, kind, outcome, error_code FROM job_attempts ORDER BY started_at DESC LIMIT 10;"
docker compose exec simulator-postgres psql -U simulator -d simulator -c \
  "SELECT id, order_id, operation_key, state FROM shipments;"
```

Look for `SUCCEEDED`, a shipment ID, and a nonempty verification time. Resend the demo event: it returns the same IDs and should not add another shipment or attempt. If a manual simulator test already created the same order using a different operation key, this create-only provider correctly returns a conflict; use a fresh order/event for the new demonstration rather than deleting evidence.

`python -m app.dispatcher --once` performs one recovery/publication cycle and exits. Logs such as `published=1` mean a queue message was sent, not that a shipment was confirmed. `UNKNOWN_OUTCOME` means verification is pending or inconclusive. `PROVIDER_CONFIGURATION` means a provider key is missing, the integration is inactive/unsupported for creation, or stored operation data cannot be validated.

Stop the four Python processes with Ctrl+C. `docker compose stop` stops infrastructure while preserving the volumes.

## Verification

```bash
export ALERTOPS_TEST_DATABASE_URL='postgresql+psycopg://alertops:alertops@localhost:5432/alertops'
export ALERTOPS_TEST_BROKER_URL='redis://127.0.0.1:6379/15'
python -m pytest -q
```

Use a development database and broker. Database tests use isolated schemas; queue tests use unique queues. The full path uses a real Celery subprocess, Redis, signed webhook acceptance, real PostgreSQL, and HTTP simulator read-back. Tests also kill a worker after remote creation and verify recovery, and isolate publication failure, duplicate execution, lease expiry, stale workers, missing confirmation, and configuration failure.

If local synchronization has created files such as `test_ingestion 2.py`, run the intended suites explicitly to avoid collecting duplicate copies:

```bash
python -m pytest -q tests/test_ingestion.py tests/test_fulfillment.py tests/test_execution.py
```

There is no deployment configuration yet. Compose runs the databases and Redis; the Python API, simulator, dispatcher, and worker run in local terminals. Production process supervision, authorization for operators, alerting, bounded retry policy, and incident management remain later milestones.
