import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey, Index, LargeBinary,
    String, UniqueConstraint, Uuid, func, text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Tenant(Base):
    __tablename__ = "tenants"
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Integration(Base):
    __tablename__ = "integrations"
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id", ondelete="RESTRICT"), index=True)
    external_key: Mapped[str] = mapped_column(String(128), unique=True)
    provider: Mapped[str] = mapped_column(String, default="mock_fulfillment")
    signing_secret_ref: Mapped[str] = mapped_column(String, default="demo_webhook_secret")
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (
        UniqueConstraint("integration_id", "external_event_id", name="uq_event_source"),
        CheckConstraint("resource_version > 0", name="ck_event_version"),
        CheckConstraint("event_type = 'order.created'", name="ck_event_type"),
        CheckConstraint("octet_length(payload_sha256) = 32", name="ck_event_hash"),
        Index("ix_event_history", "integration_id", text("received_at DESC"), "id"),
    )
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    integration_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("integrations.id", ondelete="RESTRICT"))
    external_event_id: Mapped[str] = mapped_column(String(128))
    event_type: Mapped[str] = mapped_column(String)
    resource_id: Mapped[str] = mapped_column(String(128))
    resource_version: Mapped[int] = mapped_column(BigInteger)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    payload: Mapped[dict] = mapped_column(JSONB)
    payload_sha256: Mapped[bytes] = mapped_column(LargeBinary)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (CheckConstraint("state = 'QUEUED'", name="ck_job_state"),)
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("events.id", ondelete="RESTRICT"), unique=True)
    operation_key: Mapped[str] = mapped_column(String, unique=True)
    state: Mapped[str] = mapped_column(String, server_default="QUEUED")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        UniqueConstraint("job_id", "kind", name="uq_outbox_initial"),
        CheckConstraint("kind = 'JOB_READY'", name="ck_outbox_kind"),
        Index("ix_outbox_pending", "created_at", "id", postgresql_where=text("published_at IS NULL")),
    )
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="RESTRICT"))
    kind: Mapped[str] = mapped_column(String, server_default="JOB_READY")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
