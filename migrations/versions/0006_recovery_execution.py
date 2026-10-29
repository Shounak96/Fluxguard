"""Add immutable recovery execution requests and audit events."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_recovery_execution"
down_revision: str | None = "0005_recovery_approvals"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "recovery_execution_requests",
        sa.Column("execution_id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("approval_id", sa.Uuid()),
        sa.Column("request_digest", sa.String(64), nullable=False),
        sa.Column("plan_hash", sa.String(64), nullable=False),
        sa.Column("action_type", sa.String(50), nullable=False),
        sa.Column("actor_subject", sa.String(100), nullable=False),
        sa.Column("identity_provider", sa.String(100), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "action_type IN ('REPLAY_VERIFIED_EVENT', 'RESCHEDULE_OUTBOX_EVENT', "
            "'RETAIN_QUARANTINE', 'REQUEST_MANUAL_REVIEW')",
            name="ck_recovery_execution_action",
        ),
        sa.ForeignKeyConstraint(["plan_id"], ["recovery_plans.plan_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["approval_id"], ["recovery_approvals.approval_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("execution_id"),
        sa.UniqueConstraint("execution_id", "plan_id", name="uq_recovery_execution_request_plan"),
    )
    op.create_index(
        "ix_recovery_execution_requests_plan_id", "recovery_execution_requests", ["plan_id"]
    )
    op.create_table(
        "recovery_execution_events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("execution_id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("reason_code", sa.String(100), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pre_state_digest", sa.String(64)),
        sa.Column("post_state_digest", sa.String(64)),
        sa.Column("applied_write_set", sa.JSON(), nullable=False),
        sa.Column("error_code", sa.String(100)),
        sa.Column("error_message", sa.String(500)),
        sa.CheckConstraint("sequence_number >= 1", name="ck_recovery_execution_event_sequence"),
        sa.CheckConstraint(
            "status IN ('STARTED', 'SUCCEEDED', 'NO_OP', 'FAILED')",
            name="ck_recovery_execution_event_status",
        ),
        sa.ForeignKeyConstraint(
            ["execution_id", "plan_id"],
            ["recovery_execution_requests.execution_id", "recovery_execution_requests.plan_id"],
            ondelete="RESTRICT",
            name="fk_recovery_execution_event_request",
        ),
        sa.PrimaryKeyConstraint("event_id"),
        sa.UniqueConstraint(
            "execution_id", "sequence_number", name="uq_recovery_execution_event_sequence"
        ),
    )
    op.create_index(
        "ix_recovery_execution_events_execution_id",
        "recovery_execution_events",
        ["execution_id"],
    )
    op.create_index(
        "ix_recovery_execution_events_plan_id", "recovery_execution_events", ["plan_id"]
    )
    op.create_index(
        "uq_recovery_terminal_success_per_plan",
        "recovery_execution_events",
        ["plan_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('SUCCEEDED', 'NO_OP')"),
    )
    for table in ("recovery_execution_requests", "recovery_execution_events"):
        op.execute(
            f"CREATE TRIGGER trg_{table}_append_only "
            f"BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION fluxguard_reject_recovery_mutation()"
        )


def downgrade() -> None:
    for table in ("recovery_execution_events", "recovery_execution_requests"):
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_append_only ON {table}")
    op.drop_index("uq_recovery_terminal_success_per_plan", table_name="recovery_execution_events")
    op.drop_index("ix_recovery_execution_events_plan_id", table_name="recovery_execution_events")
    op.drop_index(
        "ix_recovery_execution_events_execution_id", table_name="recovery_execution_events"
    )
    op.drop_table("recovery_execution_events")
    op.drop_index(
        "ix_recovery_execution_requests_plan_id", table_name="recovery_execution_requests"
    )
    op.drop_table("recovery_execution_requests")
