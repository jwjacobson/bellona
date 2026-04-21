"""add_source_target_property_to_relationship_types

Revision ID: 51d4d9cf5f96
Revises: 8c1fc8dcaa1c
Create Date: 2026-04-21 15:26:19.225996

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '51d4d9cf5f96'
down_revision: Union[str, Sequence[str], None] = '8c1fc8dcaa1c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'relationship_types',
        sa.Column('source_property', sa.String(length=255), nullable=True),
    )
    op.add_column(
        'relationship_types',
        sa.Column('target_property', sa.String(length=255), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('relationship_types', 'target_property')
    op.drop_column('relationship_types', 'source_property')
