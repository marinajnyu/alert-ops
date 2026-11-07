# AlertOps

AlertOps is a planned backend-first platform for practicing integration reliability, incident investigation, and safe AI-assisted operations.

> **Project status:** repository foundation. No application code, deployed service, benchmark result, or AI evaluation exists yet.

## The problem

When one system sends an operation to another, partial failures make the outcome difficult to reason about. Webhooks may be duplicated or arrive out of order. A remote API may rate-limit a request, fail temporarily, or complete an operation even though its response times out. Retrying blindly can create duplicate side effects.

AlertOps will make these failures observable, auditable, and recoverable. Its first use case is deliberately narrow: signed order events are synchronized to a simulated fulfillment provider with controllable failure modes.

## What this project is for

This repository is a learning project for practicing the full lifecycle of a production-style backend:

- API contracts, validation, authentication, and authorization
- relational modeling, constraints, transactions, indexes, and migrations
- durable asynchronous work, retries, leases, and idempotency
- reconciliation across independently changing systems
- testing with injected failures and ambiguous network outcomes
- containers, CI/CD, deployment, observability, and operations
- evidence-grounded AI investigation with read-only tools, guardrails, and evaluations

The goal is not to collect technologies or claim internet-scale reliability. Every implemented capability and reported result must be reproducible from this repository.

## Canonical v1 scenario

1. An order simulator sends a signed `order.created`, `order.updated`, or `order.cancelled` webhook.
2. AlertOps validates the request and atomically stores the event and durable work intent before returning `202 Accepted`.
3. A worker synchronizes the intended state to a mock fulfillment API using a stable idempotency key.
4. Retries handle safe transient failures; ambiguous timeouts become `UNKNOWN_OUTCOME` and require remote read-back.
5. Reconciliation compares expected and observed state and opens an incident when they remain inconsistent.
6. An operator can inspect evidence and approve a bounded recovery action.
7. In the second phase, an AI investigator can read allowlisted evidence and propose a structured diagnosis. It cannot mutate state or approve recovery.

## Engineering principles

- PostgreSQL is the authoritative business data store; Redis coordinates task delivery.
- Webhook success means durable ingestion, not completed fulfillment.
- Delivery and execution are at least once; logical side effects must be idempotent.
- A transactional outbox closes the database/queue dual-write gap.
- Deterministic code owns correctness, authorization, policy, reconciliation, and recovery execution.
- AI is advisory, evidence-linked, read-only, bounded, and evaluated against deterministic baselines.
- Start as a modular Python application with separate API, worker, and scheduler processes.
- Add infrastructure only when a measured requirement justifies it; no Kafka or Kubernetes in the initial version.

## Planned architecture

The initial stack is expected to use:

- Python, FastAPI, Pydantic, SQLAlchemy, and Alembic
- PostgreSQL for events, jobs, attempts, sync state, incidents, recovery actions, and audit history
- Redis and Celery for asynchronous delivery and scheduled work
- a separate mock fulfillment API for reproducible fault injection
- React and TypeScript for a restrained operator dashboard
- Docker Compose for local development and GitHub Actions for continuous integration
- OpenTelemetry-compatible logs, metrics, and traces

These are design choices, not claims about completed implementation. Dependencies will be added only when their milestone begins.

## Delivery phases

### Phase 1: reliable backend and operator workflow

- signed webhook ingestion and deduplication
- PostgreSQL schema and migrations
- durable jobs, transactional outbox, attempts, and bounded retry policy
- mock provider with deterministic fault injection
- reconciliation, incident lifecycle, approval, recovery, and audit history
- minimal dashboard, local containers, CI, deployment, and operational telemetry

### Phase 2: safe AI-assisted investigation

- tenant-scoped, read-only evidence tools
- schema-validated reports with evidence references and uncertainty
- server-side action allowlists and human approval boundaries
- labeled incident corpus, rule-based baseline, model evaluations, latency, and cost tracking

## Milestones

| Milestone | Outcome | Status |
| --- | --- | --- |
| 0. Repository foundation | README, scope, decisions, and contribution workflow | In progress |
| 1. Domain and database design | Invariants, ERD, table contracts, and first migration plan | Not started |
| 2. Durable ingestion | FastAPI skeleton, signed webhook, atomic event/outbox persistence, dedupe tests | Not started |
| 3. Asynchronous synchronization | Worker lifecycle, mock provider, attempts, retries, and unknown outcomes | Not started |
| 4. Reconciliation and incidents | Expected/observed state, incident workflow, recovery policy, and audit | Not started |
| 5. Operable deployment | Dashboard, containers, CI/CD, telemetry, runbook, and hosted demo | Not started |
| 6. AI investigation and evaluation | Read-only tools, guarded reports, corpus, baselines, and measured results | Not started |
| 7. Reliability proof | Failure tests, load tests, postmortem, security review, and maintenance plan | Not started |

## Definition of success

The project is complete only when the repository provides a reproducible local demo, a deployed demonstration where feasible, automated failure-recovery tests, measured benchmarks with stated limitations, operational documentation, and AI evaluation results. Hypothetical numbers will never be presented as measured results.

## Next step

Design the relational model before choosing ORM classes. The first design review will define:

- domain terms and ownership boundaries
- required invariants and allowed state transitions
- tables, keys, relationships, and uniqueness constraints
- transaction boundaries for webhook ingestion and recovery approval
- query patterns and indexes
- retention and redaction decisions for event payloads and operational evidence

That design will live in `docs/database-design.md` and be recorded through small architecture decision records before the first migration is written.

## Source specification

The project is based on the private planning document `AlertOps_Complete_Project_Specification.pdf`. This README is the public, implementation-truthful summary; the repository will not claim planned features as completed work.
