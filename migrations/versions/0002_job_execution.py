"""Guarded execution, leases, and durable attempt history."""
from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint("ck_job_state", "jobs", type_="check")
    for name, kind in (("claim_token", sa.Uuid()), ("lease_expires_at", sa.DateTime(timezone=True)),
                       ("next_check_at", sa.DateTime(timezone=True)),
                       ("provider_shipment_id", sa.String()), ("verified_at", sa.DateTime(timezone=True))):
        op.add_column("jobs", sa.Column(name, kind))
    op.add_column("jobs", sa.Column("attempt_count", sa.BigInteger(), nullable=False, server_default="0"))
    for name, expression in (
        ("ck_job_state", "state IN ('QUEUED','RUNNING','SUCCEEDED','UNKNOWN_OUTCOME','FAILED')"),
        ("ck_job_attempt_count", "attempt_count >= 0"),
        ("ck_job_claim", "(state = 'RUNNING' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL) OR (state != 'RUNNING' AND claim_token IS NULL AND lease_expires_at IS NULL)"),
        ("ck_job_verified", "state != 'SUCCEEDED' OR (provider_shipment_id IS NOT NULL AND verified_at IS NOT NULL)"),
    ):
        op.create_check_constraint(name, "jobs", expression)
    op.create_index("ix_jobs_recovery", "jobs", ["state", "next_check_at", "updated_at"])
    op.create_index("ix_jobs_lease", "jobs", ["lease_expires_at"], postgresql_where=sa.text("state = 'RUNNING'"))
    op.create_table("job_attempts",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("job_id", sa.Uuid(), sa.ForeignKey("jobs.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("attempt_number", sa.BigInteger(), nullable=False),
        sa.Column("claim_token", sa.Uuid(), nullable=False, unique=True),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("outcome", sa.String(), server_default="RUNNING", nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("error_code", sa.String()), sa.Column("http_status", sa.BigInteger()),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.UniqueConstraint("job_id", "attempt_number", name="uq_attempt_number"),
        sa.CheckConstraint("attempt_number > 0", name="ck_attempt_number"),
        sa.CheckConstraint("kind IN ('CREATE','VERIFY')", name="ck_attempt_kind"),
        sa.CheckConstraint("outcome IN ('RUNNING','SUCCEEDED','UNKNOWN_OUTCOME','FAILED','INTERRUPTED')", name="ck_attempt_outcome"),
        sa.CheckConstraint("(outcome = 'RUNNING') = (finished_at IS NULL)", name="ck_attempt_finished"),
    )
    op.create_index("ix_job_attempts_job_id", "job_attempts", ["job_id"])


def downgrade():
    # Refuse lossy downgrade once execution history exists; do not erase evidence silently.
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM job_attempts) OR EXISTS (SELECT 1 FROM jobs WHERE state <> 'QUEUED')
        THEN RAISE EXCEPTION 'Execution data exists; downgrade requires an explicit data migration';
        END IF; END $$""")
    op.drop_table("job_attempts")
    op.drop_index("ix_jobs_lease", table_name="jobs")
    op.drop_index("ix_jobs_recovery", table_name="jobs")
    for name in ("ck_job_state", "ck_job_attempt_count", "ck_job_claim", "ck_job_verified"):
        op.drop_constraint(name, "jobs", type_="check")
    for name in ("claim_token", "lease_expires_at", "next_check_at", "provider_shipment_id", "verified_at", "attempt_count"):
        op.drop_column("jobs", name)
    op.create_check_constraint("ck_job_state", "jobs", "state = 'QUEUED'")
