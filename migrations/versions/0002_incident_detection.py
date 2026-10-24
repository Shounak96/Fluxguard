"""Add durable incidents, evidence, and consumer retry state."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_incident_detection"
down_revision: str | None = "0001_milestone_one"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "incidents",
        sa.Column("incident_id", sa.Uuid(), nullable=False),
        sa.Column("incident_type", sa.String(100), nullable=False),
        sa.Column("severity", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("source_component", sa.String(100), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("occurrence_count", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(100), nullable=False),
        sa.Column("summary", sa.String(500), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.CheckConstraint("occurrence_count >= 1", name="ck_incident_occurrence_positive"),
        sa.CheckConstraint(
            "status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')",
            name="ck_incident_status",
        ),
        sa.CheckConstraint(
            "severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')",
            name="ck_incident_severity",
        ),
        sa.PrimaryKeyConstraint("incident_id"),
        sa.UniqueConstraint("fingerprint"),
    )
    op.create_index("ix_incidents_status_detected", "incidents", ["status", "detected_at"])
    op.create_index("ix_incidents_type_severity", "incidents", ["incident_type", "severity"])
    op.create_table(
        "incident_evidence",
        sa.Column("evidence_id", sa.Uuid(), nullable=False),
        sa.Column("incident_id", sa.Uuid(), nullable=False),
        sa.Column("evidence_type", sa.String(100), nullable=False),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("event_id", sa.Uuid()),
        sa.Column("kafka_topic", sa.String(255)),
        sa.Column("kafka_partition", sa.Integer()),
        sa.Column("kafka_offset", sa.BigInteger()),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("dedup_key", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(["incident_id"], ["incidents.incident_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("evidence_id"),
        sa.UniqueConstraint("dedup_key"),
    )
    op.create_index("ix_incident_evidence_event_id", "incident_evidence", ["event_id"])
    op.create_index("ix_incident_evidence_incident_id", "incident_evidence", ["incident_id"])
    op.create_index(
        "ix_incident_evidence_incident_observed",
        "incident_evidence",
        ["incident_id", "observed_at"],
    )
    op.create_table(
        "consumer_retry_state",
        sa.Column("retry_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("offset", sa.BigInteger(), nullable=False),
        sa.Column("event_id", sa.Uuid()),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("first_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error", sa.String(1000), nullable=False),
        sa.CheckConstraint("attempt_count >= 1", name="ck_consumer_retry_attempt_positive"),
        sa.PrimaryKeyConstraint("retry_id"),
        sa.UniqueConstraint("topic", "partition", "offset", name="uq_consumer_retry_record"),
    )


def downgrade() -> None:
    op.drop_table("consumer_retry_state")
    op.drop_index("ix_incident_evidence_incident_observed", table_name="incident_evidence")
    op.drop_index("ix_incident_evidence_incident_id", table_name="incident_evidence")
    op.drop_index("ix_incident_evidence_event_id", table_name="incident_evidence")
    op.drop_table("incident_evidence")
    op.drop_index("ix_incidents_type_severity", table_name="incidents")
    op.drop_index("ix_incidents_status_detected", table_name="incidents")
    op.drop_table("incidents")
