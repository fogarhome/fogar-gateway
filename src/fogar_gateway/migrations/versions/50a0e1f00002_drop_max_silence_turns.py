"""drop max_silence_turns

El silencio ya no cierra la sesión (#394): la columna sobra.

Revision ID: 50a0e1f00002
Revises: 40a0e1f00001
Create Date: 2026-11-23
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = '50a0e1f00002'
down_revision: Union[str, None] = '40a0e1f00001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table('clients') as batch_op:
        batch_op.drop_column('max_silence_turns')


def downgrade() -> None:
    with op.batch_alter_table('clients') as batch_op:
        batch_op.add_column(
            sa.Column('max_silence_turns', sa.Integer(), nullable=False, server_default='3')
        )
