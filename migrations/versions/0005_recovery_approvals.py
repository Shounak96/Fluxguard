"""Add immutable recovery approval bindings and dry-run declarations."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_recovery_approvals"
down_revision: str | None = "0004_recovery_persistence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "recovery_verifications",
        sa.Column("simulated_write_set", sa.JSON(), nullable=False, server_default="[]"),
    )
    op.add_column(
        "recovery_verifications",
        sa.Column("postconditions", sa.JSON(), nullable=False, server_default="[]"),
    )
    op.alter_column("recovery_verifications", "simulated_write_set", server_default=None)
    op.alter_column("recovery_verifications", "postconditions", server_default=None)
    op.create_table(
        "recovery_approvals",
        sa.Column("approval_id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("verification_id", sa.Uuid(), nullable=False),
        sa.Column("plan_hash", sa.String(64), nullable=False),
        sa.Column("evidence_digest", sa.String(64), nullable=False),
        sa.Column("state_digest", sa.String(64), nullable=False),
        sa.Column("policy_version", sa.String(100), nullable=False),
        sa.Column("verifier_version", sa.String(100), nullable=False),
        sa.Column("operator_subject", sa.String(255), nullable=False),
        sa.Column("identity_provider", sa.String(100), nullable=False),
        sa.Column("request_key_digest", sa.String(64), nullable=False),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("expires_at > approved_at", name="ck_recovery_approval_expiry"),
        sa.ForeignKeyConstraint(["plan_id"], ["recovery_plans.plan_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["verification_id"], ["recovery_verifications.verification_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("approval_id"),
        sa.UniqueConstraint("plan_id", name="uq_recovery_approval_plan"),
        sa.UniqueConstraint("request_key_digest", name="uq_recovery_approval_request_key"),
    )
    op.create_index("ix_recovery_approvals_plan_id", "recovery_approvals", ["plan_id"])
    op.execute(
        "CREATE TRIGGER trg_recovery_approvals_append_only "
        "BEFORE UPDATE OR DELETE ON recovery_approvals "
        "FOR EACH ROW EXECUTE FUNCTION fluxguard_reject_recovery_mutation()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_recovery_approvals_append_only ON recovery_approvals")
    op.drop_index("ix_recovery_approvals_plan_id", table_name="recovery_approvals")
    op.drop_table("recovery_approvals")
    op.drop_column("recovery_verifications", "postconditions")
    op.drop_column("recovery_verifications", "simulated_write_set")
