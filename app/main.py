import hashlib
import hmac
import json
import logging
import threading
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import ValidationError
from sqlalchemy import create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from app.config import Settings
from app.ingestion import Conflict, InactiveIntegration, OrderCreated, accept_event
from app.models import Integration

logger = logging.getLogger("alertops.ingestion")


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate JSON key")
        value[key] = item
    return value

def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    sessions = sessionmaker(engine)
    rate_lock = threading.Lock()
    # Fixed window per API process; shared/global rate limiting comes with deployment.
    rate_window = [0, 0]

    @asynccontextmanager
    async def lifespan(app):
        yield
        engine.dispose()

    app = FastAPI(title="AlertOps", lifespan=lifespan)
    app.state.sessions = sessions

    @app.middleware("http")
    async def correlation(request: Request, call_next):
        request.state.correlation_id = str(uuid.uuid4())
        response = await call_next(request)
        response.headers["X-Request-Id"] = request.state.correlation_id
        logger.info(json.dumps({"request_id": request.state.correlation_id,
                               "status": response.status_code, "path": request.url.path}))
        return response

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/api/v1/webhooks/orders", status_code=202)
    async def webhook(request: Request):
        with rate_lock:
            window = int(time.monotonic() // 60)
            if rate_window[0] != window:
                rate_window[:] = [window, 0]
            rate_window[1] += 1
            if rate_window[1] > settings.requests_per_minute:
                raise HTTPException(429, "Rate limit exceeded", headers={"Retry-After": "60"})
        raw = bytearray()
        async for chunk in request.stream():
            if len(raw) + len(chunk) > settings.max_body_bytes:
                raise HTTPException(413, "Request body too large")
            raw.extend(chunk)
        # Run synchronous database work in the thread pool, not on the event loop.
        from starlette.concurrency import run_in_threadpool
        return await run_in_threadpool(process, request, bytes(raw))

    def process(request: Request, raw: bytes):
        try:
            with sessions() as session:
                with session.begin():
                    integration = session.scalar(select(Integration).where(
                        Integration.external_key == request.headers.get("X-Integration-Id", "")
                    ).with_for_update(read=True))
                    if (integration is None or not integration.active
                            or integration.signing_secret_ref != "demo_webhook_secret"):
                        raise HTTPException(401, "Invalid integration credentials")
                    signature = request.headers.get("X-Signature-SHA256", "")
                    expected = hmac.new(settings.webhook_secret.get_secret_value().encode(),
                                        raw, hashlib.sha256).hexdigest()
                    if not hmac.compare_digest(signature.encode(), expected.encode()):
                        raise HTTPException(401, "Invalid integration credentials")
                    try:
                        parsed = json.loads(raw, object_pairs_hook=unique_object,
                                            parse_constant=lambda _: reject_constant())
                        body = OrderCreated.model_validate(parsed)
                        if request.headers.get("X-Event-Id") != body.event_id:
                            raise ValueError("Event IDs do not match")
                    except (ValueError, UnicodeError, ValidationError, RecursionError):
                        raise HTTPException(422, "Invalid order event") from None
                    accepted = accept_event(session, integration.id, body)
                return accepted
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None
        except InactiveIntegration:
            raise HTTPException(401, "Invalid integration credentials") from None
        except SQLAlchemyError:
            # Do not log SQL parameters or database credentials.
            raise HTTPException(503, "Event acceptance unavailable; retry later") from None

    return app


def reject_constant():
    raise ValueError("Non-finite JSON number")
