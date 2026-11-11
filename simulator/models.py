import uuid
from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, DateTime, LargeBinary, String, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Shipment(Base):
    __tablename__ = "shipments"
    __table_args__ = (
        UniqueConstraint("account_id", "operation_key", name="uq_shipment_operation"),
        UniqueConstraint("account_id", "order_id", name="uq_shipment_order"),
        CheckConstraint("resource_version > 0", name="ck_shipment_version"),
        CheckConstraint("item_count > 0", name="ck_shipment_count"),
        CheckConstraint("state = 'CREATED'", name="ck_shipment_state"),
        CheckConstraint("octet_length(request_hash) = 32", name="ck_shipment_hash"),
    )
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    account_id: Mapped[str] = mapped_column(String(128))
    operation_key: Mapped[str] = mapped_column(String(256))
    order_id: Mapped[str] = mapped_column(String(128))
    resource_version: Mapped[int] = mapped_column(BigInteger)
    item_count: Mapped[int] = mapped_column(BigInteger)
    request_hash: Mapped[bytes] = mapped_column(LargeBinary)
    state: Mapped[str] = mapped_column(String, server_default="CREATED")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
