import hashlib
import hmac
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select, text

from app.config import Settings
from app.ingestion import OrderCreated, canonical_json
from app.main import create_app, unique_object
from app.models import Event, Integration, Job, OutboxEvent, Tenant

SECRET = "test-only-signing-secret"
BODY = {"event_id": "evt_001", "type": "order.created", "order_id": "ord_1042",
        "resource_version": 1, "occurred_at": "2026-11-07T18:00:00Z", "data": {"item_count": 3}}


def test_schema_rejects_coercion():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        OrderCreated.model_validate({**BODY, "resource_version": True})


def test_duplicate_keys_rejected():
    with pytest.raises(ValueError):
        json.loads('{"data":{"item_count":1,"item_count":2}}', object_pairs_hook=unique_object)


def test_canonical_json_fixture():
    assert canonical_json({"b": 2, "a": 1}) == b'{"a":1,"b":2}'


@pytest.fixture
def client(monkeypatch):
    url = os.getenv("ALERTOPS_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set ALERTOPS_TEST_DATABASE_URL to run real PostgreSQL integration tests")
    # Use a fresh schema in the supplied database; never truncate existing user data.
    schema = "test_" + uuid.uuid4().hex
    admin = create_engine(url)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    from sqlalchemy.engine import make_url
    scoped_url = make_url(url).update_query_dict({"options": f"-csearch_path={schema}"})
    scoped = scoped_url.render_as_string(hide_password=False)
    monkeypatch.setenv("ALERTOPS_DATABASE_URL", scoped)
    monkeypatch.setenv("ALERTOPS_WEBHOOK_SECRET", SECRET)
    command.upgrade(Config("alembic.ini"), "head")
    app = create_app(Settings(database_url=scoped, webhook_secret=SECRET))
    with app.state.sessions() as session, session.begin():
        tenant = Tenant(name="Test tenant")
        session.add(tenant)
        session.flush()
        session.add(Integration(tenant_id=tenant.id, external_key="demo-store-01"))
    try:
        with TestClient(app) as http:
            yield http, app
    finally:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def send(http, body=None, raw=None, **headers):
    raw = raw if raw is not None else json.dumps(body or BODY).encode()
    values = {"X-Integration-Id": "demo-store-01", "X-Event-Id": (body or BODY)["event_id"],
              "X-Signature-SHA256": hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest()}
    return http.post("/api/v1/webhooks/orders", content=raw, headers=values | headers)


def counts(app):
    with app.state.sessions() as session:
        return [session.scalar(select(func.count()).select_from(model))
                for model in (Event, Job, OutboxEvent)]


def test_accept_and_retry(client):
    http, app = client
    first = send(http)
    assert first.status_code == 202
    for _ in range(10):
        assert send(http).json() == first.json()
    assert send(http, raw=json.dumps(BODY, sort_keys=True, indent=2).encode()).json() == first.json()
    assert counts(app) == [1, 1, 1]


def test_concurrent_duplicates(client):
    http, app = client
    with ThreadPoolExecutor(max_workers=8) as pool:
        replies = list(pool.map(lambda _: send(http), range(10)))
    assert all(reply.status_code == 202 for reply in replies)
    assert len({reply.json()["job_id"] for reply in replies}) == 1
    assert counts(app) == [1, 1, 1]


def test_conflicts_roll_back(client):
    http, app = client
    assert send(http).status_code == 202
    assert send(http, {**BODY, "data": {"item_count": 9}}).status_code == 409
    assert send(http, {**BODY, "event_id": "evt_other"}).status_code == 409
    assert counts(app) == [1, 1, 1]


@pytest.mark.parametrize("headers,raw,status", [
    ({"X-Signature-SHA256": "bad"}, None, 401),
    ({"X-Integration-Id": "missing"}, None, 401),
    ({"X-Event-Id": "different"}, None, 422),
    ({}, b'{"event_id":"x","event_id":"y"}', 422),
    ({}, b'{', 422),
    ({}, b'x' * 65537, 413),
])
def test_rejections_leave_no_rows(client, headers, raw, status):
    http, app = client
    assert send(http, raw=raw, **headers).status_code == status
    assert counts(app) == [0, 0, 0]


def test_database_events_immutable(client):
    from sqlalchemy.exc import DBAPIError
    http, app = client
    send(http)
    with app.state.sessions() as session:
        with pytest.raises(DBAPIError):
            with session.begin():
                session.execute(text("UPDATE events SET resource_version = 2"))


def test_transaction_rolls_back_on_outbox_failure(client, monkeypatch):
    from sqlalchemy import event
    from sqlalchemy.exc import OperationalError
    http, app = client

    def fail(*args):
        raise OperationalError("injected failure", {}, Exception("test"))

    event.listen(OutboxEvent, "before_insert", fail)
    try:
        assert send(http).status_code == 503
        assert counts(app) == [0, 0, 0]
    finally:
        event.remove(OutboxEvent, "before_insert", fail)
    assert send(http).status_code == 202


def test_migrations_match_models(client):
    command.check(Config("alembic.ini"))


def test_equivalent_timezone_retry(client):
    http, app = client
    first = send(http)
    retry = send(http, {**BODY, "occurred_at": "2026-11-07T10:00:00-08:00"})
    assert retry.status_code == 202
    assert retry.json() == first.json()
    assert counts(app) == [1, 1, 1]


def test_inactive_integration(client):
    http, app = client
    with app.state.sessions() as session, session.begin():
        integration = session.scalar(select(Integration))
        integration.active = False
    assert send(http).status_code == 401
    assert counts(app) == [0, 0, 0]


def test_rate_limit(client):
    _, app = client
    database_url = app.state.sessions.kw["bind"].url.render_as_string(hide_password=False)
    limited = create_app(Settings(database_url=database_url,
                                  webhook_secret=SECRET, requests_per_minute=1))
    with TestClient(limited) as http:
        assert http.post("/api/v1/webhooks/orders", content=b"{}").status_code == 401
        response = http.post("/api/v1/webhooks/orders", content=b"{}")
        assert response.status_code == 429
        assert response.headers["Retry-After"] == "60"
    assert counts(app) == [0, 0, 0]


def test_migration_round_trip(client):
    config = Config("alembic.ini")
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    command.check(config)
