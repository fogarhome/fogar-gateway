"""notify device: homes.notify_client_id, memberships.notify_client_id

El dispositivo de avisos (5.0, #407): el del hogar y el de cada persona, FK a `clients` con
ON DELETE SET NULL. No se rellena: tras actualizar queda NULL hasta que alguien lo elija.

Revision ID: 50a0e1f00004
Revises: 50a0e1f00003
Create Date: 2026-11-25
"""
from typing import Sequence, Union

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = '50a0e1f00004'
down_revision: Union[str, None] = '50a0e1f00003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# SQLite por lotes necesita las restricciones con nombre para poder quitarlas al bajar.
_FKS = (
    ('homes', 'fk_homes_notify_client_id_clients'),
    ('memberships', 'fk_memberships_notify_client_id_clients'),
)


def upgrade() -> None:
    for table, fk in _FKS:
        with op.batch_alter_table(table) as batch_op:
            batch_op.add_column(sa.Column('notify_client_id', sqlmodel.sql.sqltypes.AutoString(), nullable=True))
            batch_op.create_foreign_key(
                fk, 'clients', ['notify_client_id'], ['id'], ondelete='SET NULL'
            )


def downgrade() -> None:
    for table, fk in _FKS:
        with op.batch_alter_table(table) as batch_op:
            batch_op.drop_constraint(fk, type_='foreignkey')
            batch_op.drop_column('notify_client_id')
