"""P0 application schema and exact-search knowledge vectors."""
from alembic import op
from app.db import Base
from app import models
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    Base.metadata.create_all(op.get_bind())

def downgrade():
    raise RuntimeError("Destructive downgrade disabled; restore a scoped backup explicitly.")
