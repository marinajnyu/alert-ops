# Fulfillment workflow: create and confirm a simulated shipment

Status: signed event acceptance, the persistent simulator, and its HTTP adapter are implemented. Dispatch, worker execution, leases, and recovery scans below remain planned.

## Meaning and scope

Fulfillment is what happens after an order is placed: preparing, packing, and shipping its items. AlertOps will coordinate a much smaller operation: create a shipment record in an external provider and confirm that the provider stored it. A confirmed record does not mean a package was physically shipped or delivered.

The initial order event supplies an order ID, resource version, and item count. These are sufficient for a synthetic shipment record. Real purchases, addresses, shipping labels, payments, and carrier bookings are outside this slice.

The next milestone is: one accepted order produces one confirmed simulator shipment, a recorded execution attempt, and a `SUCCEEDED` job, even when the task is delivered repeatedly.

## Chosen components

- A dispatcher publishes existing PostgreSQL outbox entries to Redis/Celery.
- A worker claims a job in PostgreSQL, calls a provider adapter over HTTP, and records the outcome.
- A separate fulfillment simulator persists its own shipments and idempotency records in PostgreSQL. Local development uses a dedicated PostgreSQL container and volume for clearer isolation. AlertOps accesses simulator state only through HTTP.
- An adapter hides provider-specific authentication, endpoints, and response formats from business logic. Start with `create_shipment(operation)` and `get_shipment(operation_key)` methods returning validated domain results.

The simulator is a stateful dependency, not a function returning canned success. Its state survives process restarts. Tests will eventually inject realistic failures through it. A real sandbox adapter is a later milestone; its idempotency and lookup guarantees must be verified rather than assumed to match the simulator.

## Successful path

1. The existing webhook transaction commits an event, queued job, and outbox entry; the API returns `202`.
2. The dispatcher selects an unpublished entry with a row lock, publishes a task containing only the job ID, and records `published_at` after successful broker acknowledgment. Use a bounded publication timeout and small batches; do not hold a large batch locked during network calls.
3. The worker loads the job and derives its tenant, integration, and event from PostgreSQL. It does not trust tenant or operation arguments supplied by the queue.
4. In a short transaction, the worker conditionally claims a `QUEUED` job, changes it to `RUNNING`, assigns a claim token and lease deadline, and creates its attempt record. Commit before HTTP calls.
5. The adapter sends the stored order data and existing operation key to the simulator.
6. The simulator atomically persists its shipment and idempotency mapping, then returns a shipment ID.
7. The worker reads the shipment back by operation key. It validates the returned operation key, order ID, version, item count, and `CREATED` state against the expected operation.
8. In another transaction, the worker completes the attempt and marks the job `SUCCEEDED`, storing the provider shipment ID and verification time. The update must match the active claim token and an unexpired lease.
9. The worker acknowledges the task after its outcome is durably recorded. A repeated delivery for a succeeded job exits without making another creation request.

No PostgreSQL transaction spans the provider HTTP request. Queue publication does not mean job completion, and an HTTP creation response alone does not satisfy our read-back verification criterion.

## Simulator API contract

All provider endpoints require a simulator API key distinct from the webhook signing secret. Server-side configuration maps each key to a provider account namespace associated with the integration. The namespace is never taken from an untrusted request field.

### Create a shipment

```http
POST /v1/shipments
Authorization: Bearer <simulator-account-key>
Idempotency-Key: <existing job.operation_key>
Content-Type: application/json
```

```json
{
  "order_id": "ord_1042",
  "resource_version": 1,
  "item_count": 3
}
```

First creation returns `201`; a matching idempotent replay returns `200`, both with the same body:

```json
{
  "shipment_id": "<provider UUID>",
  "operation_key": "fulfillment:create:<existing digest>",
  "order_id": "ord_1042",
  "resource_version": 1,
  "item_count": 3,
  "state": "CREATED"
}
```

- Unique `(account_id, operation_key)` and a canonical request hash prevent duplicate effects. Concurrent matching requests return the same shipment. Reusing the key with different content returns `409`.
- Unique `(account_id, order_id)` permits one shipment per order for this create-only slice. A different create key for an already represented order returns `409`; future order updates must use a separate update contract, not create another shipment.
- Shipment and idempotency state commit together. Never return creation success before commit. Idempotency records remain available for the lifetime of this demo; early expiry would invalidate our retry guarantee.
- Normal reads are immediately consistent after successful creation. Delayed visibility is a later fault-injection mode.

### Read back by operation key

```http
GET /v1/shipments/by-operation?operation_key=<URL-encoded operation key>
Authorization: Bearer <simulator-account-key>
```

Return `200` with the same shipment representation, or `404` when no record is visible in that authenticated account. A lookup must never reveal another account's shipment.

## Required AlertOps schema evolution

Create migration `0002`; preserve the existing initial migration.

| Change | Purpose |
| --- | --- |
| Expand job states to `QUEUED`, `RUNNING`, `SUCCEEDED`, `UNKNOWN_OUTCOME`, `FAILED` | Express execution and uncertainty; retry scheduling follows later |
| Add `claim_token`, `lease_expires_at`, `attempt_count`, `provider_shipment_id`, `verified_at` to jobs | Fence concurrent/stale workers and retain confirmation |
| Add `job_attempts` with job FK, unique `(job_id, attempt_number)`, claim token, start/finish times, outcome, sanitized error code, HTTP status, and correlation ID | Preserve execution evidence independently of current job state |
| Index queued/stale jobs and expired running leases | Permit bounded recovery scans without relying on broker state |

Use a guarded update or row lock to allocate an attempt number and claim atomically. Attempts are created once and finalized once; preserve finalized evidence. A duplicate delivery while a claim is active does not allocate another attempt or call the provider. Provider credentials and raw sensitive response bodies never enter attempt records.

Start with a 60-second lease, a 5-second timeout per provider request, and no automatic HTTP-client retries. These are configuration defaults to validate under tests, not measured performance guarantees. A stale worker cannot complete local state after its lease or claim has been replaced. Provider-side idempotency still protects remote effects when local fencing cannot stop an already transmitted request.

## Failure boundaries included from the start

| Condition | Required behavior |
| --- | --- |
| Redis unavailable during publication | Leave outbox unpublished; dispatcher tries on a later poll |
| Dispatcher dies after publish but before recording it | Republish safely; the worker must tolerate duplicate messages |
| Broker loses a published task | A periodic scan of stale `QUEUED` jobs makes their existing outbox entries eligible for republishing; no second logical job is created |
| Creation times out or its response is malformed | Record `UNKNOWN_OUTCOME`; query remote state before another side-effecting request |
| Read-back fails or returns an unexpected record | Never mark success; retain uncertainty or a classified conflict |
| Worker dies after remote creation | On lease expiry, recovery records an interrupted attempt, moves the job to `UNKNOWN_OUTCOME`, and schedules read-only verification |
| Recovery lookup confirms the expected shipment | Complete verification and mark success through a fresh guarded claim |
| Recovery lookup returns `404` or cannot establish the outcome | Keep `UNKNOWN_OUTCOME`; do not automatically recreate in this slice |
| Provider explicitly rejects the request, such as validation/authentication conflict | Record a sanitized failure; do not repeat it automatically |
| Provider returns `429` or `5xx` | Record the response. Do not assume every `5xx` proves no effect; use read-back where outcome is uncertain. Automated backoff/retry policy follows in the next slice |

Recovery scans are part of this slice's durability requirements. They must use conditional state updates and claims so multiple scanners cannot create competing owners. Unknown-outcome verification needs its own claimed execution and recorded evidence; it must never authorize a new create call implicitly. Configure Celery delivery/acknowledgment behavior during implementation and prove it with process-failure tests; delivery settings alone are not the correctness mechanism.

## Proof before declaring this slice complete

- A real HTTP test runs webhook → outbox → Redis/Celery → simulator → read-back with PostgreSQL on both sides, without eager-mode tasks or a stubbed provider.
- Assertions check one event, one logical job, one simulator shipment, completed attempt evidence, and a verified `SUCCEEDED` job.
- Repeated and concurrent task delivery produces one remote shipment. A succeeded job makes no further creation request.
- Simulator restart preserves both shipment and idempotency state. Changed payload under an existing key conflicts.
- Provider account isolation is tested, including the same source order ID in two separate accounts.
- Killing dispatcher/worker processes at publication and remote-success boundaries still permits recovery without a second shipment.
- A lost task is redispatched; an expired worker cannot overwrite a newer claim.
- Failure or absence of remote verification never becomes success. Queue outage does not break durable webhook acceptance.

## Delivery order and deferred work

Implement the persistent simulator and adapter contract tests first. Then add the schema migration, guarded worker, outbox dispatcher, recovery scans, and real-stack tests. Extend Compose and CI as each component becomes runnable.

After this slice, implement class-specific retry/backoff, delayed visibility, scheduled reconciliation, incident creation, and the real provider sandbox adapter. AI investigation depends on that operational evidence and comes afterward.
