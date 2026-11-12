"""Durable claims and outcomes. Network operations never hold a database transaction."""
import uuid
from dataclasses import dataclass
from datetime import timedelta

from pydantic import ValidationError
from sqlalchemy import func, select

from app.models import Event, Integration, Job, JobAttempt
from app.providers.fulfillment import (
    FulfillmentAdapter, LookupUnavailable, Operation, ProviderRejected, UnknownOutcome,
)


@dataclass(frozen=True)
class Claim:
    job_id: uuid.UUID
    token: uuid.UUID
    kind: str
    operation: Operation | None
    account_key: str


def claim_job(sessions, settings, job_id: uuid.UUID) -> Claim | None:
    with sessions() as session, session.begin():
        now = session.scalar(select(func.clock_timestamp()))
        job = session.scalar(select(Job).where(Job.id == job_id).with_for_update(skip_locked=True))
        if job is None or job.state not in ("QUEUED", "UNKNOWN_OUTCOME"):
            return None
        if job.next_check_at and job.next_check_at > now:
            return None
        kind = "CREATE" if job.state == "QUEUED" else "VERIFY"
        event = session.get(Event, job.event_id)
        integration = session.get(Integration, event.integration_id)
        operation = None
        if integration.provider == "mock_fulfillment" and (integration.active or kind == "VERIFY"):
            try:
                operation = Operation(operation_key=job.operation_key, order_id=event.resource_id,
                                      resource_version=event.resource_version,
                                      item_count=event.payload["data"]["item_count"])
            except (ValidationError, KeyError, TypeError):
                pass
        token = uuid.uuid4()
        job.state = "RUNNING"
        job.claim_token = token
        job.lease_expires_at = now + timedelta(seconds=settings.lease_seconds)
        job.attempt_count += 1
        job.updated_at = now
        job.next_check_at = None
        session.add(JobAttempt(job_id=job.id, attempt_number=job.attempt_count,
                               claim_token=token, kind=kind))
        return Claim(job.id, token, kind, operation, integration.external_key)


def finish(sessions, settings, claim, state, *, shipment=None, code=None, http_status=None):
    with sessions() as session, session.begin():
        job = session.scalar(select(Job).where(Job.id == claim.job_id).with_for_update())
        now = session.scalar(select(func.clock_timestamp()))
        if (job.state != "RUNNING" or job.claim_token != claim.token
                or job.lease_expires_at <= now):
            return False
        attempt = session.scalar(select(JobAttempt).where(JobAttempt.claim_token == claim.token))
        attempt.outcome = state
        attempt.finished_at = now
        attempt.error_code = code
        attempt.http_status = http_status
        job.state = state
        job.claim_token = None
        job.lease_expires_at = None
        job.updated_at = now
        job.next_check_at = (now + timedelta(seconds=settings.verification_seconds)
                             if state == "UNKNOWN_OUTCOME" else None)
        if shipment is not None:
            job.provider_shipment_id = str(shipment.shipment_id)
            job.verified_at = now
        return True


def execute_job(sessions, settings, job_id, adapter_factory=FulfillmentAdapter):
    claim = claim_job(sessions, settings, job_id)
    if claim is None:
        return "SKIPPED"
    key = settings.provider_keys.get(claim.account_key)
    if claim.operation is None or key is None or not key.get_secret_value():
        # A verification configuration failure cannot prove a prior create failed.
        state = "FAILED" if claim.kind == "CREATE" else "UNKNOWN_OUTCOME"
        finish(sessions, settings, claim, state, code="PROVIDER_CONFIGURATION")
        return state
    with adapter_factory(settings.provider_url, key.get_secret_value()) as adapter:
        if claim.kind == "CREATE":
            try:
                adapter.create_shipment(claim.operation)
            except ProviderRejected as exc:
                finish(sessions, settings, claim, "FAILED", code="PROVIDER_REJECTED",
                       http_status=exc.status_code)
                return "FAILED"
            except UnknownOutcome:
                # A read may confirm remote success even though the create response was lost.
                pass
        try:
            shipment = adapter.verify_shipment(claim.operation)
        except (LookupUnavailable, ProviderRejected):
            shipment = None
        if shipment is None:
            finish(sessions, settings, claim, "UNKNOWN_OUTCOME", code="REMOTE_NOT_CONFIRMED")
            return "UNKNOWN_OUTCOME"
        return ("SUCCEEDED" if finish(sessions, settings, claim, "SUCCEEDED", shipment=shipment)
                else "STALE_CLAIM")
