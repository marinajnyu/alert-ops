"""Celery delivers IDs; PostgreSQL remains the source of execution state."""
import uuid
from functools import lru_cache

from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from app.execution import execute_job
from app.execution_config import ExecutionSettings


def make_celery(settings):
    app = Celery("alertops", broker=settings.broker_url)
    app.conf.update(
        task_default_queue=settings.task_queue, task_serializer="json", accept_content=["json"],
        task_acks_late=True, task_reject_on_worker_lost=True, task_ignore_result=True,
        worker_prefetch_multiplier=1, broker_connection_timeout=3,
        broker_transport_options={"visibility_timeout": 120, "socket_connect_timeout": 3,
                                  "socket_timeout": 3, "max_retries": 0},
        task_publish_retry=False, broker_connection_retry_on_startup=True,
    )
    return app


@lru_cache
def runtime():
    settings = ExecutionSettings()
    # Created lazily inside worker processes, not before a prefork worker forks.
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    return settings, sessionmaker(engine)


celery_app = make_celery(ExecutionSettings())


@celery_app.task(name="alertops.fulfill")
def fulfill(job_id: str):
    try:
        parsed = uuid.UUID(job_id)
    except (ValueError, TypeError, AttributeError):
        return "INVALID_ID"
    settings, sessions = runtime()
    try:
        return execute_job(sessions, settings, parsed)
    except SQLAlchemyError:
        # Never print SQL parameters/credentials. Dispatcher recovery repairs abandoned work.
        raise RuntimeError("Execution database unavailable; durable recovery required") from None
