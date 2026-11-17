"""Estado compartilhado do provedor financeiro de teste."""

from alembic import op
import sqlalchemy as sa

revision = "20261117_fake_provider"
down_revision = "154c337f9574"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "fake_provider_state",
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("estado", sa.JSON(), nullable=False),
    )


def downgrade():
    op.drop_table("fake_provider_state")
