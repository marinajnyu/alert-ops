"""Persist shipments and account-scoped idempotency together."""
from alembic import op
import sqlalchemy as sa

revision = "sim_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "shipments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("account_id", sa.String(128), nullable=False),
        sa.Column("operation_key", sa.String(256), nullable=False),
        sa.Column("order_id", sa.String(128), nullable=False),
        sa.Column("resource_version", sa.BigInteger(), nullable=False),
        sa.Column("item_count", sa.BigInteger(), nullable=False),
        sa.Column("request_hash", sa.LargeBinary(), nullable=False),
        sa.Column("state", sa.String(), server_default="CREATED", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("account_id", "operation_key", name="uq_shipment_operation"),
        sa.UniqueConstraint("account_id", "order_id", name="uq_shipment_order"),
        sa.CheckConstraint("resource_version > 0", name="ck_shipment_version"),
        sa.CheckConstraint("item_count > 0", name="ck_shipment_count"),
        sa.CheckConstraint("state = 'CREATED'", name="ck_shipment_state"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_shipment_hash"),
    )


def downgrade():
    op.drop_table("shipments")
