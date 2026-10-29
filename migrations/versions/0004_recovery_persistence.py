"""Add immutable recovery plans and append-only verification audit."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_recovery_persistence"
down_revision: str | None = "0003_ai_investigations"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LIFECYCLE_STATES = (
    "'PROPOSED', 'VERIFIED', 'AWAITING_APPROVAL', 'APPROVED', 'EXECUTING', "
    "'SUCCEEDED', 'NO_OP', 'FAILED', 'EXPIRED', 'REJECTED'"
)


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_incident_evidence_id_incident",
        "incident_evidence",
        ["evidence_id", "incident_id"],
    )
    op.create_table(
        "recovery_plans",
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("plan_hash", sa.String(64), nullable=False),
        sa.Column("incident_id", sa.Uuid(), nullable=False),
        sa.Column("action_type", sa.String(50), nullable=False),
        sa.Column("target", sa.JSON(), nullable=False),
        sa.Column("canonical_inputs", sa.JSON(), nullable=False),
        sa.Column("evidence_digest", sa.String(64), nullable=False),
        sa.Column("expected_event_sha256", sa.String(64)),
        sa.Column("state_digest", sa.String(64), nullable=False),
        sa.Column("policy_version", sa.String(100), nullable=False),
        sa.Column("source", sa.String(30), nullable=False),
        sa.Column("created_by", sa.String(100), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "action_type IN ('REPLAY_VERIFIED_EVENT', 'RESCHEDULE_OUTBOX_EVENT', "
            "'RETAIN_QUARANTINE', 'REQUEST_MANUAL_REVIEW')",
            name="ck_recovery_plan_action",
        ),
        sa.CheckConstraint(
            "source IN ('OPERATOR', 'INVESTIGATION', 'SYSTEM')",
            name="ck_recovery_plan_source",
        ),
        sa.ForeignKeyConstraint(["incident_id"], ["incidents.incident_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("plan_id"),
        sa.UniqueConstraint("plan_hash"),
        sa.UniqueConstraint("plan_id", "incident_id", name="uq_recovery_plan_id_incident"),
    )
    op.create_index("ix_recovery_plans_incident_id", "recovery_plans", ["incident_id"])
    op.create_table(
        "recovery_plan_evidence",
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("evidence_id", sa.Uuid(), nullable=False),
        sa.Column("incident_id", sa.Uuid(), nullable=False),
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(
            ["plan_id", "incident_id"],
            ["recovery_plans.plan_id", "recovery_plans.incident_id"],
            ondelete="RESTRICT",
            name="fk_recovery_evidence_plan_incident",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_id", "incident_id"],
            ["incident_evidence.evidence_id", "incident_evidence.incident_id"],
            ondelete="RESTRICT",
            name="fk_recovery_evidence_incident_owner",
        ),
        sa.PrimaryKeyConstraint("plan_id", "evidence_id"),
    )
    op.create_table(
        "recovery_verifications",
        sa.Column("verification_id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("decision", sa.String(30), nullable=False),
        sa.Column("reason_codes", sa.JSON(), nullable=False),
        sa.Column("expected_effect", sa.String(50), nullable=False),
        sa.Column("verifier_version", sa.String(100), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state_digest", sa.String(64), nullable=False),
        sa.Column("validated_event_id", sa.Uuid()),
        sa.Column("validated_aggregate_id", sa.Uuid()),
        sa.CheckConstraint(
            "decision IN ('ELIGIBLE', 'INELIGIBLE', 'INSUFFICIENT_EVIDENCE')",
            name="ck_recovery_verification_decision",
        ),
        sa.ForeignKeyConstraint(["plan_id"], ["recovery_plans.plan_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("verification_id"),
        sa.UniqueConstraint("plan_id", name="uq_recovery_verification_plan"),
    )
    op.create_index("ix_recovery_verifications_plan_id", "recovery_verifications", ["plan_id"])
    op.create_table(
        "recovery_lifecycle_events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("from_state", sa.String(30)),
        sa.Column("to_state", sa.String(30), nullable=False),
        sa.Column("reason_code", sa.String(100), nullable=False),
        sa.Column("actor_type", sa.String(30), nullable=False),
        sa.Column("actor_id", sa.String(100), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("sequence_number >= 1", name="ck_recovery_lifecycle_sequence"),
        sa.CheckConstraint(
            f"from_state IS NULL OR from_state IN ({_LIFECYCLE_STATES})",
            name="ck_recovery_lifecycle_from_state",
        ),
        sa.CheckConstraint(
            f"to_state IN ({_LIFECYCLE_STATES})",
            name="ck_recovery_lifecycle_to_state",
        ),
        sa.CheckConstraint(
            "actor_type IN ('OPERATOR', 'INVESTIGATION', 'SYSTEM')",
            name="ck_recovery_lifecycle_actor",
        ),
        sa.ForeignKeyConstraint(["plan_id"], ["recovery_plans.plan_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("event_id"),
        sa.UniqueConstraint(
            "plan_id", "sequence_number", name="uq_recovery_lifecycle_plan_sequence"
        ),
    )
    op.create_index(
        "ix_recovery_lifecycle_events_plan_id", "recovery_lifecycle_events", ["plan_id"]
    )
    op.execute(
        """
        CREATE FUNCTION fluxguard_reject_recovery_mutation()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'recovery audit records are append-only';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    for table in (
        "recovery_plans",
        "recovery_plan_evidence",
        "recovery_verifications",
        "recovery_lifecycle_events",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table}_append_only "
            f"BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION fluxguard_reject_recovery_mutation()"
        )


def downgrade() -> None:
    for table in (
        "recovery_lifecycle_events",
        "recovery_verifications",
        "recovery_plan_evidence",
        "recovery_plans",
    ):
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_append_only ON {table}")
    op.execute("DROP FUNCTION IF EXISTS fluxguard_reject_recovery_mutation()")
    op.drop_index("ix_recovery_lifecycle_events_plan_id", table_name="recovery_lifecycle_events")
    op.drop_table("recovery_lifecycle_events")
    op.drop_index("ix_recovery_verifications_plan_id", table_name="recovery_verifications")
    op.drop_table("recovery_verifications")
    op.drop_table("recovery_plan_evidence")
    op.drop_index("ix_recovery_plans_incident_id", table_name="recovery_plans")
    op.drop_table("recovery_plans")
    op.drop_constraint("uq_incident_evidence_id_incident", "incident_evidence", type_="unique")
