import hashlib
import hmac
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from types import SimpleNamespace

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from app.config import Settings
from app.dispatcher import dispatch_once, recover
from app.execution import claim_job, execute_job, finish
from app.execution_config import ExecutionSettings
from app.main import create_app
from app.models import Integration, Job, JobAttempt, OutboxEvent, Tenant
from app.providers.fulfillment import FulfillmentAdapter, UnknownOutcome
from app.task_queue import make_celery
from test_fulfillment import live_server
from simulator.config import SimulatorSettings
from simulator.models import Shipment as RemoteShipment


@pytest.fixture
def stack(monkeypatch):
    url = os.getenv("ALERTOPS_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set ALERTOPS_TEST_DATABASE_URL for PostgreSQL execution tests")
    admin = create_engine(url)
    schemas = ["execution_" + uuid.uuid4().hex, "provider_" + uuid.uuid4().hex]
    urls = []
    for schema in schemas:
        with admin.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        urls.append(make_url(url).update_query_dict({"options": f"-csearch_path={schema}"})
                    .render_as_string(hide_password=False))
    monkeypatch.setenv("ALERTOPS_DATABASE_URL", urls[0])
    monkeypatch.setenv("ALERTOPS_WEBHOOK_SECRET", "execution-signature-secret")
    monkeypatch.setenv("SIMULATOR_DATABASE_URL", urls[1])
    monkeypatch.setenv("SIMULATOR_ACCOUNTS", '{"demo-store-01":"provider-secret"}')
    engine = create_engine(urls[0])
    sessions = sessionmaker(engine)
    try:
        command.upgrade(Config("alembic.ini"), "head")
        command.upgrade(Config("simulator/alembic.ini"), "head")
        with sessions() as session, session.begin():
            tenant = Tenant(name="Execution demo")
            session.add(tenant)
            session.flush()
            session.add(Integration(tenant_id=tenant.id, external_key="demo-store-01"))
        with live_server(SimulatorSettings(database_url=urls[1], accounts={"demo-store-01": "provider-secret"})) as (provider_url, provider):
            settings = ExecutionSettings(database_url=urls[0], provider_url=provider_url,
                provider_keys={"demo-store-01": "provider-secret"}, recovery_seconds=1,
                verification_seconds=1)
            api = create_app(Settings(database_url=urls[0], webhook_secret="execution-signature-secret"))
            with TestClient(api) as client:
                raw = json.dumps({"event_id": "execution-event", "type": "order.created",
                    "order_id": "execution-order", "resource_version": 1,
                    "occurred_at": "2026-11-11T18:00:00Z", "data": {"item_count": 3}}).encode()
                response = client.post("/api/v1/webhooks/orders", content=raw, headers={
                    "X-Integration-Id": "demo-store-01", "X-Event-Id": "execution-event",
                    "X-Signature-SHA256": hmac.new(b"execution-signature-secret", raw, hashlib.sha256).hexdigest()})
                assert response.status_code == 202
                job_id = uuid.UUID(response.json()["job_id"])
                yield SimpleNamespace(sessions=sessions, settings=settings, job_id=job_id,
                    provider=provider, api=api, client=client)
    finally:
        engine.dispose()
        for schema in reversed(schemas):
            with admin.begin() as connection:
                connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def remote_count(stack):
    with stack.provider.state.sessions() as session:
        return session.scalar(select(func.count()).select_from(RemoteShipment))


def test_success_and_concurrent_duplicate_execution(stack):
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: execute_job(stack.sessions, stack.settings, stack.job_id), range(8)))
    assert results.count("SUCCEEDED") == 1
    assert execute_job(stack.sessions, stack.settings, stack.job_id) == "SKIPPED"
    assert remote_count(stack) == 1
    with stack.sessions() as session:
        job = session.get(Job, stack.job_id)
        assert job.state == "SUCCEEDED" and job.verified_at and job.provider_shipment_id
        assert session.scalar(select(func.count()).select_from(JobAttempt)) == 1


def test_broker_failure_retains_pending_work(stack):
    def fail(_):
        raise ConnectionError("broker offline")
    with pytest.raises(ConnectionError):
        dispatch_once(stack.sessions, fail)
    with stack.sessions() as session:
        assert session.scalar(select(OutboxEvent.published_at)) is None
        assert session.get(Job, stack.job_id).state == "QUEUED"


def test_publish_then_crash_repeats_safely(stack):
    delivered = []
    def publish_then_die(job_id):
        delivered.append(job_id)
        raise RuntimeError("dispatcher crashed after publication")
    with pytest.raises(RuntimeError):
        dispatch_once(stack.sessions, publish_then_die)
    dispatch_once(stack.sessions, delivered.append)
    assert len(delivered) == 2
    for job_id in delivered:
        execute_job(stack.sessions, stack.settings, uuid.UUID(job_id))
    assert remote_count(stack) == 1


def test_worker_expiry_verifies_remote_effect_and_fences_old_worker(stack):
    claim = claim_job(stack.sessions, stack.settings, stack.job_id)
    with FulfillmentAdapter(stack.settings.provider_url, "provider-secret") as adapter:
        shipment = adapter.create_shipment(claim.operation)
    with stack.sessions() as session, session.begin():
        job = session.get(Job, stack.job_id)
        job.lease_expires_at = session.scalar(select(func.clock_timestamp())) - timedelta(seconds=1)
    assert not finish(stack.sessions, stack.settings, claim, "SUCCEEDED", shipment=shipment)
    assert recover(stack.sessions, stack.settings) == 1
    assert execute_job(stack.sessions, stack.settings, stack.job_id) == "SUCCEEDED"
    assert remote_count(stack) == 1
    with stack.sessions() as session:
        attempts = session.scalars(select(JobAttempt).order_by(JobAttempt.attempt_number)).all()
        assert [(a.kind, a.outcome) for a in attempts] == [("CREATE", "INTERRUPTED"), ("VERIFY", "SUCCEEDED")]
    assert not finish(stack.sessions, stack.settings, claim, "FAILED")


def test_missing_readback_never_recreates(stack):
    class MissingProvider:
        def __init__(self, *args):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def create_shipment(self, operation):
            raise UnknownOutcome("response lost")
        def verify_shipment(self, operation):
            return None
    assert execute_job(stack.sessions, stack.settings, stack.job_id, MissingProvider) == "UNKNOWN_OUTCOME"
    with stack.sessions() as session, session.begin():
        job = session.get(Job, stack.job_id)
        job.next_check_at = session.scalar(select(func.clock_timestamp()))
    assert execute_job(stack.sessions, stack.settings, stack.job_id) == "UNKNOWN_OUTCOME"
    assert remote_count(stack) == 0
    with stack.sessions() as session:
        assert session.get(Job, stack.job_id).state == "UNKNOWN_OUTCOME"


def test_lost_message_becomes_publishable_again(stack):
    assert dispatch_once(stack.sessions, lambda _: None) == 1
    with stack.sessions() as session, session.begin():
        past = session.scalar(select(func.clock_timestamp())) - timedelta(seconds=5)
        session.get(Job, stack.job_id).updated_at = past
        session.scalar(select(OutboxEvent)).published_at = past
    recover(stack.sessions, stack.settings)
    published = []
    assert dispatch_once(stack.sessions, published.append) == 1
    assert published == [str(stack.job_id)]


def test_misconfigured_provider_fails_without_remote_effect(stack):
    stack.settings.provider_keys = {}
    assert execute_job(stack.sessions, stack.settings, stack.job_id) == "FAILED"
    assert remote_count(stack) == 0


def test_rejected_credentials_record_status_and_stop(stack):
    from pydantic import SecretStr
    stack.settings.provider_keys = {"demo-store-01": SecretStr("wrong-key")}
    assert execute_job(stack.sessions, stack.settings, stack.job_id) == "FAILED"
    with stack.sessions() as session:
        attempt = session.scalar(select(JobAttempt))
        assert attempt.http_status == 401
        assert attempt.error_code == "PROVIDER_REJECTED"
    assert remote_count(stack) == 0


@pytest.mark.parametrize("failure", [None, "dispatcher_crash", "worker_crash", "lost_task"])
def test_real_celery_worker_completes_signed_webhook(stack, tmp_path, failure):
    broker = os.getenv("ALERTOPS_TEST_BROKER_URL")
    if not broker:
        pytest.skip("Set ALERTOPS_TEST_BROKER_URL for real Redis/Celery integration test")
    settings = stack.settings.model_copy(update={"broker_url": broker,
                                                  "task_queue": "test." + uuid.uuid4().hex})
    env = dict(os.environ, ALERTOPS_DATABASE_URL=settings.database_url,
        ALERTOPS_BROKER_URL=broker, ALERTOPS_TASK_QUEUE=settings.task_queue,
        ALERTOPS_PROVIDER_URL=settings.provider_url,
        ALERTOPS_PROVIDER_KEYS='{"demo-store-01":"provider-secret"}')
    queue = make_celery(settings)
    read_started = threading.Event()
    allow_read = threading.Event()
    if failure == "worker_crash":
        route = next(route for route in stack.provider.routes
                     if getattr(route, "path", None) == "/v1/shipments/by-operation")
        original = route.dependant.call

        def pause_read(**kwargs):
            read_started.set()
            assert allow_read.wait(timeout=25), "Test did not release provider read"
            return original(**kwargs)

        route.dependant.call = pause_read

    def start_worker(log):
        return subprocess.Popen([sys.executable, "-m", "celery", "-A", "app.task_queue:celery_app",
            "worker", "--pool=solo", "--concurrency=1", "--without-gossip", "--without-mingle",
            "--without-heartbeat", "--loglevel=ERROR"], env=env, stdout=log, stderr=log)

    def publish(job_id):
        queue.send_task("alertops.fulfill", args=[job_id], retry=False)

    # This is a real subprocess and real Redis delivery, never eager mode.
    with (tmp_path / "worker.log").open("w+") as log:
        worker = start_worker(log)
        try:
            if failure == "dispatcher_crash":
                def publish_then_die(job_id):
                    publish(job_id)
                    raise RuntimeError("Publication succeeded but dispatcher did not commit")
                with pytest.raises(RuntimeError):
                    dispatch_once(stack.sessions, publish_then_die)
            elif failure == "lost_task":
                dispatch_once(stack.sessions, lambda _: None)
                with stack.sessions() as session, session.begin():
                    past = session.scalar(select(func.clock_timestamp())) - timedelta(seconds=5)
                    session.get(Job, stack.job_id).updated_at = past
                    session.scalar(select(OutboxEvent)).published_at = past
                recover(stack.sessions, settings)
            if failure is None:
                result = subprocess.run([sys.executable, "-m", "app.dispatcher", "--once"],
                    env=env, capture_output=True, text=True, timeout=15)
                assert result.returncode == 0, "Dispatcher command failed"
            else:
                dispatch_once(stack.sessions, publish)
            if failure == "worker_crash":
                assert read_started.wait(timeout=20), "Worker did not reach remote verification"
                assert remote_count(stack) == 1
                worker.kill()
                worker.wait(timeout=5)
                allow_read.set()
                with stack.sessions() as session, session.begin():
                    session.get(Job, stack.job_id).lease_expires_at = (
                        session.scalar(select(func.clock_timestamp())) - timedelta(seconds=1))
                    session.scalar(select(OutboxEvent)).published_at = (
                        session.scalar(select(func.clock_timestamp())) - timedelta(seconds=5))
                assert recover(stack.sessions, settings) == 1
                dispatch_once(stack.sessions, publish)
                worker = start_worker(log)
            deadline = time.monotonic() + 25
            while time.monotonic() < deadline:
                with stack.sessions() as session:
                    if session.get(Job, stack.job_id).state == "SUCCEEDED":
                        break
                assert worker.poll() is None, "Celery worker exited unexpectedly"
                time.sleep(0.1)
            else:
                pytest.fail("Real Celery worker did not complete the job")
            queue.send_task("alertops.fulfill", args=[str(stack.job_id)], retry=False)
            assert remote_count(stack) == 1
            with stack.sessions() as session:
                job = session.get(Job, stack.job_id)
                assert job.verified_at is not None
                assert job.attempt_count == (2 if failure == "worker_crash" else 1)
        finally:
            allow_read.set()
            worker.terminate()
            try:
                worker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=5)
            queue.control.purge()
            queue.close()
