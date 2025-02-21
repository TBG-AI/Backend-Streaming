"""initial_state

Revision ID: e5d72e401bc2
Revises: 
Create Date: 2025-02-20 14:59:49.740458

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5d72e401bc2'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Tables already exist, this is just marking the initial state
    pass


def downgrade() -> None:
    # No downgrade needed for initial state
    pass 