import os
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import httpx
import pytest
import uvicorn
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url

from app.providers.fulfillment import (
    FulfillmentAdapter, LookupUnavailable, Operation, ProviderRejected, UnknownOutcome,
)
from simulator.config import SimulatorSettings
from simulator.main import create_app
from simulator.models import Shipment

OPERATION = Operation(operation_key="fulfillment:create:demo", order_id="ord_1042",
                      resource_version=1, item_count=3)


@contextmanager
def live_server(settings):
    app = create_app(settings)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="critical", lifespan="on"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not server.started:
            if not thread.is_alive() or time.monotonic() > deadline:
                raise RuntimeError("Simulator failed to start")
            time.sleep(0.01)
        yield f"http://127.0.0.1:{port}", app
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()
        assert not thread.is_alive(), "Simulator did not stop"


@pytest.fixture
def simulator_settings(monkeypatch):
    url = os.getenv("ALERTOPS_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set ALERTOPS_TEST_DATABASE_URL for PostgreSQL simulator integration tests")
    schema = "simtest_" + uuid.uuid4().hex
    admin = create_engine(url)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    scoped = make_url(url).update_query_dict({"options": f"-csearch_path={schema}"})
    database_url = scoped.render_as_string(hide_password=False)
    monkeypatch.setenv("SIMULATOR_DATABASE_URL", database_url)
    monkeypatch.setenv("SIMULATOR_ACCOUNTS", '{"account-a":"key-a","account-b":"key-b"}')
    try:
        command.upgrade(Config("simulator/alembic.ini"), "head")
        yield SimulatorSettings(database_url=database_url,
                                accounts={"account-a": "key-a", "account-b": "key-b"})
    finally:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def test_create_replay_readback_and_restart(simulator_settings):
    for restart in range(2):
        with live_server(simulator_settings) as (url, app):
            with FulfillmentAdapter(url, "key-a") as adapter:
                if restart == 0:
                    assert adapter.get_shipment(OPERATION.operation_key) is None
                    original = adapter.create_shipment(OPERATION)
                replay = adapter.create_shipment(OPERATION)
                assert replay == original
                assert adapter.verify_shipment(OPERATION) == original
            with app.state.sessions() as session:
                assert session.scalar(select(func.count()).select_from(Shipment)) == 1


def test_concurrent_requests_have_one_effect(simulator_settings):
    with live_server(simulator_settings) as (url, app):
        with FulfillmentAdapter(url, "key-a") as adapter:
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(lambda _: adapter.create_shipment(OPERATION), range(16)))
        assert len({item.shipment_id for item in results}) == 1
        with app.state.sessions() as session:
            assert session.scalar(select(func.count()).select_from(Shipment)) == 1


def test_conflicts_and_account_isolation(simulator_settings):
    with live_server(simulator_settings) as (url, _):
        with (FulfillmentAdapter(url, "key-a") as first,
              FulfillmentAdapter(url, "key-b") as second,
              FulfillmentAdapter(url, "wrong") as unauthorized):
            shipment = first.create_shipment(OPERATION)
            assert second.get_shipment(OPERATION.operation_key) is None
            other = second.create_shipment(OPERATION)
            assert shipment.shipment_id != other.shipment_id
            for changed in ({"item_count": 4}, {"operation_key": "another-create-key"}):
                with pytest.raises(ProviderRejected) as error:
                    first.create_shipment(OPERATION.model_copy(update=changed))
                assert error.value.status_code == 409
            assert first.verify_shipment(OPERATION) == shipment
            with pytest.raises(ProviderRejected) as error:
                unauthorized.create_shipment(OPERATION)
            assert error.value.status_code == 401


def test_raw_contract_validation_and_status_codes(simulator_settings):
    with live_server(simulator_settings) as (url, _):
        headers = {"Authorization": "Bearer key-a", "Idempotency-Key": OPERATION.operation_key}
        body = OPERATION.model_dump(exclude={"operation_key"})
        with httpx.Client(base_url=url, trust_env=False) as client:
            assert client.post("/v1/shipments", headers=headers, json=body).status_code == 201
            assert client.post("/v1/shipments", headers=headers, json=body).status_code == 200
            assert client.post("/v1/shipments", json=body).status_code == 401
            for invalid in ({**body, "item_count": 0}, {**body, "resource_version": True},
                            {**body, "unexpected": "field"}):
                assert client.post("/v1/shipments", headers=headers, json=invalid).status_code == 422


def test_simulator_migration_round_trip(simulator_settings):
    config = Config("simulator/alembic.ini")
    command.check(config)
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    command.check(config)


def test_committed_creation_with_lost_response_recovers_by_readback(simulator_settings):
    with live_server(simulator_settings) as (url, app):
        with httpx.Client(base_url=url, trust_env=False) as network:
            def lose_response(request):
                response = network.send(request)
                assert response.status_code == 201
                raise httpx.ReadTimeout("Response lost after commit", request=request)

            transport = httpx.MockTransport(lose_response)
            with FulfillmentAdapter(url, "key-a", transport=transport) as adapter:
                with pytest.raises(UnknownOutcome):
                    adapter.create_shipment(OPERATION)
        with FulfillmentAdapter(url, "key-a") as adapter:
            assert adapter.verify_shipment(OPERATION).order_id == OPERATION.order_id
        with app.state.sessions() as session:
            assert session.scalar(select(func.count()).select_from(Shipment)) == 1


@pytest.mark.parametrize("status", [500, 502, 302])
def test_uncertain_creation_never_retries(status):
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(status)

    with FulfillmentAdapter("http://provider", "secret", transport=httpx.MockTransport(respond)) as a:
        with pytest.raises(UnknownOutcome):
            a.create_shipment(OPERATION)
    assert len(calls) == 1


def test_lost_response_and_lookup_failure_are_distinct():
    def timeout(request):
        raise httpx.ReadTimeout("sensitive details", request=request)

    with FulfillmentAdapter("http://provider", "secret", transport=httpx.MockTransport(timeout)) as a:
        with pytest.raises(UnknownOutcome) as error:
            a.create_shipment(OPERATION)
        assert "sensitive" not in str(error.value)
        with pytest.raises(LookupUnavailable):
            a.get_shipment(OPERATION.operation_key)


@pytest.mark.parametrize("body", [b"not-json", b'{"state":"CREATED"}'])
def test_invalid_creation_response_is_unknown(body):
    transport = httpx.MockTransport(lambda _: httpx.Response(201, content=body))
    with FulfillmentAdapter("http://provider", "secret", transport=transport) as adapter:
        with pytest.raises(UnknownOutcome):
            adapter.create_shipment(OPERATION)


def test_verification_rejects_wrong_order():
    payload = {**OPERATION.model_dump(), "shipment_id": str(uuid.uuid4()), "state": "CREATED",
               "order_id": "wrong-order"}
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    with FulfillmentAdapter("http://provider", "secret", transport=transport) as adapter:
        with pytest.raises(LookupUnavailable):
            adapter.verify_shipment(OPERATION)


def test_credentials_are_required_and_distinct():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        SimulatorSettings(accounts={"a": "same", "b": "same"})
