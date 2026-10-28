"""Add persistent AI investigation runs."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_ai_investigations"
down_revision: str | None = "0002_incident_detection"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "investigation_runs",
        sa.Column("investigation_id", sa.Uuid(), nullable=False),
        sa.Column("incident_id", sa.Uuid(), nullable=False),
        sa.Column("model_id", sa.String(255), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("diagnosis", sa.JSON()),
        sa.Column("tool_call_history", sa.JSON(), nullable=False),
        sa.Column("prompt_tokens", sa.Integer()),
        sa.Column("completion_tokens", sa.Integer()),
        sa.Column("total_tokens", sa.Integer()),
        sa.Column("estimated_cost_usd", sa.Numeric(12, 6)),
        sa.Column("llm_call_count", sa.Integer(), nullable=False),
        sa.Column("error_category", sa.String(100)),
        sa.Column("error_message", sa.String(500)),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'INSUFFICIENT_EVIDENCE', "
            "'UNVERIFIED', 'FAILED')",
            name="ck_investigation_status",
        ),
        sa.CheckConstraint("llm_call_count >= 0", name="ck_investigation_call_count_nonnegative"),
        sa.ForeignKeyConstraint(["incident_id"], ["incidents.incident_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("investigation_id"),
    )
    op.create_index(
        "ix_investigation_incident_created",
        "investigation_runs",
        ["incident_id", "created_at"],
    )
    op.create_index("ix_investigation_runs_incident_id", "investigation_runs", ["incident_id"])


def downgrade() -> None:
    op.drop_index("ix_investigation_runs_incident_id", table_name="investigation_runs")
    op.drop_index("ix_investigation_incident_created", table_name="investigation_runs")
    op.drop_table("investigation_runs")
