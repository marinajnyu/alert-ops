"""Initial durable ingestion schema."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    def identity():
        return sa.Column("id", sa.Uuid(), primary_key=True)

    def timestamp(name):
        return sa.Column(name, sa.DateTime(timezone=True), nullable=False,
                         server_default=sa.func.now())

    def reference(name, target, unique=False):
        return sa.Column(name, sa.Uuid(), sa.ForeignKey(target, ondelete="RESTRICT"),
                         nullable=False, unique=unique)

    op.create_table("tenants", identity(), sa.Column("name", sa.String(), nullable=False),
                    timestamp("created_at"))
    op.create_table("integrations", identity(), reference("tenant_id", "tenants.id"),
                    sa.Column("external_key", sa.String(128), nullable=False, unique=True),
                    sa.Column("provider", sa.String(), nullable=False),
                    sa.Column("signing_secret_ref", sa.String(), nullable=False),
                    sa.Column("active", sa.Boolean(), nullable=False), timestamp("created_at"))
    op.create_index("ix_integrations_tenant_id", "integrations", ["tenant_id"])
    op.create_table("events", identity(), reference("integration_id", "integrations.id"),
                    sa.Column("external_event_id", sa.String(128), nullable=False),
                    sa.Column("event_type", sa.String(), nullable=False),
                    sa.Column("resource_id", sa.String(128), nullable=False),
                    sa.Column("resource_version", sa.BigInteger(), nullable=False),
                    sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
                    timestamp("received_at"), sa.Column("payload", JSONB(), nullable=False),
                    sa.Column("payload_sha256", sa.LargeBinary(), nullable=False),
                    sa.UniqueConstraint("integration_id", "external_event_id", name="uq_event_source"),
                    sa.CheckConstraint("resource_version > 0", name="ck_event_version"),
                    sa.CheckConstraint("event_type = 'order.created'", name="ck_event_type"),
                    sa.CheckConstraint("octet_length(payload_sha256) = 32", name="ck_event_hash"))
    op.create_index("ix_event_history", "events", ["integration_id", sa.text("received_at DESC"), "id"])
    op.create_table("jobs", identity(), reference("event_id", "events.id", unique=True),
                    sa.Column("operation_key", sa.String(), nullable=False, unique=True),
                    sa.Column("state", sa.String(), nullable=False, server_default="QUEUED"),
                    timestamp("created_at"), timestamp("updated_at"),
                    sa.CheckConstraint("state = 'QUEUED'", name="ck_job_state"))
    op.create_table("outbox_events", identity(), reference("job_id", "jobs.id"),
                    sa.Column("kind", sa.String(), nullable=False, server_default="JOB_READY"),
                    timestamp("created_at"), sa.Column("published_at", sa.DateTime(timezone=True)),
                    sa.UniqueConstraint("job_id", "kind", name="uq_outbox_initial"),
                    sa.CheckConstraint("kind = 'JOB_READY'", name="ck_outbox_kind"))
    op.create_index("ix_outbox_pending", "outbox_events", ["created_at", "id"],
                    postgresql_where=sa.text("published_at IS NULL"))
    op.execute("""
        CREATE FUNCTION reject_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'Accepted events are immutable'; END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER immutable_events BEFORE UPDATE OR DELETE ON events
        FOR EACH ROW EXECUTE FUNCTION reject_event_mutation()
    """)


def downgrade():
    for table in ("outbox_events", "jobs", "events", "integrations", "tenants"):
        op.drop_table(table)
    op.execute("DROP FUNCTION reject_event_mutation()")
