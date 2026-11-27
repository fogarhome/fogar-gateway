"""client scopes: clients.scopes

Permisos por aparato (5.0, #410). Cada aparato recibe su tope: uno del hogar y uno de una persona
miembro, `conversations:read` + `conversations:write`; uno de un dueño o administrador, los cuatro.

Revision ID: 50a0e1f00005
Revises: 50a0e1f00004
Create Date: 2026-11-26
"""
from typing import Sequence, Union

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = '50a0e1f00005'
down_revision: Union[str, None] = '50a0e1f00004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Literales: una migración no importa el código vivo, que puede cambiar.
_DEFAULT = '["conversations:read", "conversations:write"]'
_ALL = '["conversations:read", "conversations:write", "home:admin", "home:read"]'


def upgrade() -> None:
    with op.batch_alter_table('clients') as batch_op:
        batch_op.add_column(
            sa.Column(
                'scopes', sqlmodel.sql.sqltypes.AutoString(), nullable=False,
                server_default=_DEFAULT,
            )
        )
    op.execute(
        sa.text(
            "UPDATE clients SET scopes = :all WHERE user_id IS NOT NULL AND EXISTS ("
            " SELECT 1 FROM memberships m WHERE m.user_id = clients.user_id"
            " AND m.home_id = clients.home_id AND m.role IN ('owner', 'admin'))"
        ).bindparams(all=_ALL)
    )


def downgrade() -> None:
    with op.batch_alter_table('clients') as batch_op:
        batch_op.drop_column('scopes')
