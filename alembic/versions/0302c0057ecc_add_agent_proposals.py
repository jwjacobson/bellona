"""add_agent_proposals

Revision ID: 0302c0057ecc
Revises: f968e8a293ce
Create Date: 2026-03-10 20:14:58.374183

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0302c0057ecc'
down_revision: Union[str, Sequence[str], None] = 'f968e8a293ce'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'agent_proposals',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('proposal_type', sa.String(length=50), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('content', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('confidence', sa.Float(), nullable=True),
        sa.Column('connector_id', sa.Uuid(), nullable=True),
        sa.Column('entity_type_id', sa.Uuid(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['connector_id'], ['connectors.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['entity_type_id'], ['entity_types.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('agent_proposals')
