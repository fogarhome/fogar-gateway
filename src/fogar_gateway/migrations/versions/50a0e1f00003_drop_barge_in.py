"""drop barge_in_enabled, barge_in_min_chars

El barge-in sale del gateway (5.0, #406): hablar no corta el turno; pausar o callar la voz lo
decide el cliente en local. Las dos columnas sobran.

Revision ID: 50a0e1f00003
Revises: 50a0e1f00002
Create Date: 2026-11-24
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = '50a0e1f00003'
down_revision: Union[str, None] = '50a0e1f00002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table('clients') as batch_op:
        batch_op.drop_column('barge_in_min_chars')
        batch_op.drop_column('barge_in_enabled')


def downgrade() -> None:
    # Los valores por defecto de antes (True, 5) para las filas que ya existen.
    with op.batch_alter_table('clients') as batch_op:
        batch_op.add_column(
            sa.Column('barge_in_enabled', sa.Boolean(), nullable=False, server_default=sa.true())
        )
        batch_op.add_column(
            sa.Column('barge_in_min_chars', sa.Integer(), nullable=False, server_default='5')
        )
