"""Timestamp stored knowledge versions; do not claim original source authoring dates."""
from alembic import op
revision="0002"
down_revision="0001"
branch_labels=None
depends_on=None
def upgrade():
    op.execute("ALTER TABLE knowledge ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()")
def downgrade():
    raise RuntimeError("Destructive downgrades are disabled; restore an approved backup instead.")
