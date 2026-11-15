"""Poll durable work, repair interrupted claims, and publish job IDs."""
import argparse
import logging
import time
from datetime import timedelta

from sqlalchemy import create_engine, func, or_, select
from sqlalchemy.orm import sessionmaker

from app.execution_config import ExecutionSettings
from app.models import Job, JobAttempt, OutboxEvent
from app.task_queue import make_celery

logger = logging.getLogger("alertops.dispatcher")


def recover(sessions, settings, limit=100):
    with sessions() as session, session.begin():
        now = session.scalar(select(func.clock_timestamp()))
        expired = session.scalars(select(Job).where(
            Job.state == "RUNNING", Job.lease_expires_at <= now
        ).order_by(Job.lease_expires_at, Job.id).limit(limit).with_for_update(skip_locked=True)).all()
        for job in expired:
            attempt = session.scalar(select(JobAttempt).where(JobAttempt.claim_token == job.claim_token))
            attempt.outcome = "INTERRUPTED"
            attempt.finished_at = now
            attempt.error_code = "LEASE_EXPIRED"
            job.state = "UNKNOWN_OUTCOME"
            job.claim_token = None
            job.lease_expires_at = None
            job.next_check_at = now
            job.updated_at = now
        session.flush()
        # Broker durability is not assumed: old queued/verification work can be republished.
        due = session.scalars(select(Job).join(OutboxEvent, OutboxEvent.job_id == Job.id).where(
            OutboxEvent.published_at <= now - timedelta(seconds=settings.recovery_seconds), or_(
            (Job.state == "QUEUED") & (Job.updated_at <= now - timedelta(seconds=settings.recovery_seconds)),
            (Job.state == "UNKNOWN_OUTCOME") & (Job.next_check_at <= now),
        )).order_by(OutboxEvent.published_at, Job.id).limit(limit)
            .with_for_update(of=Job, skip_locked=True)).all()
        for job in due:
            outbox = session.scalar(select(OutboxEvent).where(OutboxEvent.job_id == job.id)
                                    .with_for_update(skip_locked=True))
            if outbox and (outbox.published_at is None or outbox.published_at <=
                           now - timedelta(seconds=settings.recovery_seconds)):
                outbox.published_at = None
        return len(expired)


def dispatch_once(sessions, publish, limit=100):
    published = 0
    for _ in range(limit):
        with sessions() as session, session.begin():
            now = session.scalar(select(func.clock_timestamp()))
            entry = session.scalar(select(OutboxEvent).join(Job, Job.id == OutboxEvent.job_id).where(
                OutboxEvent.published_at.is_(None), or_(Job.state == "QUEUED",
                    (Job.state == "UNKNOWN_OUTCOME") & (Job.next_check_at <= now)),
            ).order_by(OutboxEvent.created_at, OutboxEvent.id).limit(1)
              .with_for_update(of=OutboxEvent, skip_locked=True))
            if entry is None:
                break
            # One row held during bounded broker I/O. Publish first; duplicates after a crash are safe.
            publish(str(entry.job_id))
            entry.published_at = session.scalar(select(func.clock_timestamp()))
            published += 1
    return published


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    settings = ExecutionSettings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    sessions = sessionmaker(engine)
    queue = make_celery(settings)

    def publish(job_id):
        queue.send_task("alertops.fulfill", args=[job_id], retry=False)

    try:
        while True:
            try:
                recovered = recover(sessions, settings)
                published = dispatch_once(sessions, publish)
                logger.info("recovered=%d published=%d", recovered, published)
            except Exception:
                # Preserve pending work and avoid logging connection strings or payloads.
                logger.error("Dispatch cycle failed; pending work retained")
                if args.once:
                    raise SystemExit(1) from None
            if args.once:
                break
            time.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        queue.close()
        engine.dispose()


if __name__ == "__main__":
    main()
