"""cleanup_event_id_constraint

Revision ID: e5d72e401bc2
Revises: a287ae1c905b
Create Date: 2025-02-20 14:59:49.740458

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5d72e401bc2'
down_revision: Union[str, None] = 'a287ae1c905b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop redundant constraints if they exist
    op.execute("""
        DO $$
        BEGIN
            -- Drop unique constraint if it exists
            IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_match_projection_event_id') THEN
                ALTER TABLE match_projection DROP CONSTRAINT uq_match_projection_event_id;
            END IF;
            
            -- Drop primary key if it exists
            IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'match_projection_pkey') THEN
                ALTER TABLE match_projection DROP CONSTRAINT match_projection_pkey;
            END IF;
        END $$;
    """)
    
    # Add primary key constraint
    op.execute("""
        ALTER TABLE match_projection 
        ADD CONSTRAINT match_projection_pkey PRIMARY KEY (event_id);
    """)


def downgrade() -> None:
    # Add back the unique constraint in downgrade
    op.create_unique_constraint(
        'uq_match_projection_event_id',
        'match_projection',
        ['event_id']
    )