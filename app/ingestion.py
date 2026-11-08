import hashlib
import json
import uuid
from datetime import timezone
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import Event, Integration, Job, OutboxEvent

Identifier = Annotated[str, StringConstraints(min_length=1, max_length=128, pattern=r"^\S+$")]
PositiveInt = Annotated[int, Field(strict=True, gt=0, le=9223372036854775807)]


class OrderData(BaseModel):
    model_config = ConfigDict(extra="forbid")
    item_count: PositiveInt


class OrderCreated(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: Identifier
    type: Literal["order.created"]
    order_id: Identifier
    resource_version: PositiveInt
    occurred_at: AwareDatetime
    data: OrderData


class Conflict(Exception):
    pass


class InactiveIntegration(Exception):
    pass


def canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()

# creates an event and a job for the given integration, or returns the existing event and job if the event already exists
def accept_event(session: Session, integration_id: uuid.UUID, body: OrderCreated) -> dict:
    integration = session.scalar(
        select(Integration).where(Integration.id == integration_id).with_for_update(read=True)
    )
    if integration is None or not integration.active:
        raise InactiveIntegration
    payload = body.model_dump(mode="json")
    payload["occurred_at"] = body.occurred_at.astimezone(timezone.utc).isoformat()
    digest = hashlib.sha256(canonical_json(payload)).digest()
    event_id = session.scalar(
        insert(Event).values(
            id=uuid.uuid4(), integration_id=integration_id, external_event_id=body.event_id,
            event_type=body.type, resource_id=body.order_id, resource_version=body.resource_version,
            occurred_at=body.occurred_at, payload=payload, payload_sha256=digest,
        ).on_conflict_do_nothing(constraint="uq_event_source").returning(Event.id)
    )
    if event_id is None:
        existing = session.scalar(select(Event).where(
            Event.integration_id == integration_id, Event.external_event_id == body.event_id
        ))
        if existing.payload_sha256 != digest:
            raise Conflict("Event ID already used with different content")
        job = session.scalar(select(Job).where(Job.event_id == existing.id))
        return result(existing.id, body.event_id, job.id)
    operation = [str(integration_id), "fulfillment.create", body.order_id, body.resource_version]
    operation_key = "fulfillment:create:" + hashlib.sha256(canonical_json(operation)).hexdigest()
    job_id = session.scalar(insert(Job).values(
        id=uuid.uuid4(), event_id=event_id, operation_key=operation_key
    ).on_conflict_do_nothing(index_elements=["operation_key"]).returning(Job.id))
    if job_id is None:
        raise Conflict("Logical operation already accepted under another event ID")

    # Add an OutboxEvent to the session to ensure that the job is processed asynchronously
    session.add(OutboxEvent(job_id=job_id))
    # Flush the session to ensure that the OutboxEvent is persisted before returning the result
    session.flush()
    
    return result(event_id, body.event_id, job_id)


def result(event_id: uuid.UUID, external_id: str, job_id: uuid.UUID) -> dict:
    return {"event_id": str(event_id), "external_event_id": external_id,
            "job_id": str(job_id), "status": "accepted"}
