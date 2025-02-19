"""add_event_id_unique_constraint

Revision ID: a287ae1c905b
Revises: None
Create Date: 2025-02-19 15:26:43.089350

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a287ae1c905b'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add unique constraint
    op.create_unique_constraint(
        'uq_match_projection_event_id',
        'match_projection',
        ['event_id']
    )


def downgrade() -> None:
    # Remove unique constraint
    op.drop_constraint(
        'uq_match_projection_event_id',
        'match_projection',
        type_='unique'
    )
