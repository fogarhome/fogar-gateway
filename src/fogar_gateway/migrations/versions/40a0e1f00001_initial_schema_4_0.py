"""4.0 initial schema (hogar, usuarios y dispositivos, M20)

Sustituye a las revisiones de la 3.x: una BD 3.x no se convierte (instalación nueva).

Revision ID: 40a0e1f00001
Revises: 
Create Date: 2026-11-21 12:04:31.688088

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel

from fogar_gateway.db.models import DEFAULT_HOME_ID, DEFAULT_HOME_NAME


# revision identifiers, used by Alembic.
revision: str = '40a0e1f00001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('homes',
    sa.Column('id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('name', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('agent', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    sa.Column('created_at', sqlmodel.sql.sqltypes.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    # El hogar de esta instalación (M20): existe siempre.
    op.execute(
        sa.text("INSERT INTO homes (id, name, created_at) VALUES (:id, :name, CURRENT_TIMESTAMP)")
        .bindparams(id=DEFAULT_HOME_ID, name=DEFAULT_HOME_NAME)
    )
    op.create_table('users',
    sa.Column('id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('username', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('display_name', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('password_hash', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('created_at', sqlmodel.sql.sqltypes.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_users_username'), 'users', ['username'], unique=True)
    op.create_table('clients',
    sa.Column('id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('home_id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('user_id', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    sa.Column('name', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('key_hash', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('client_type', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    sa.Column('default_agent', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    sa.Column('allowed_agents', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    sa.Column('capabilities', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    sa.Column('created_at', sqlmodel.sql.sqltypes.UTCDateTime(), nullable=False),
    sa.Column('last_seen_at', sqlmodel.sql.sqltypes.UTCDateTime(), nullable=True),
    sa.Column('retired_at', sqlmodel.sql.sqltypes.UTCDateTime(), nullable=True),
    sa.Column('stt_language', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('stt_vad_thold', sa.Float(), nullable=False),
    sa.Column('tts_voice', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('tts_speed', sa.Float(), nullable=False),
    sa.Column('barge_in_enabled', sa.Boolean(), nullable=False),
    sa.Column('barge_in_min_chars', sa.Integer(), nullable=False),
    sa.Column('output_mode', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    sa.Column('silence_timeout_s', sa.Float(), nullable=False),
    sa.Column('max_silence_turns', sa.Integer(), nullable=False),
    sa.Column('push_enabled', sa.Boolean(), nullable=False),
    sa.Column('tool_calls_enabled', sa.Boolean(), nullable=False),
    sa.ForeignKeyConstraint(['home_id'], ['homes.id'], ),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('id', 'home_id', name='uq_clients_id_home')
    )
    op.create_index(op.f('ix_clients_key_hash'), 'clients', ['key_hash'], unique=True)
    op.create_index(op.f('ix_clients_home_id'), 'clients', ['home_id'], unique=False)
    op.create_index(op.f('ix_clients_user_id'), 'clients', ['user_id'], unique=False)
    op.create_table('memberships',
    sa.Column('user_id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('home_id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('role', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('agent', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    sa.ForeignKeyConstraint(['home_id'], ['homes.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('user_id', 'home_id')
    )
    op.create_table('conversations',
    sa.Column('id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('home_id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('client_id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('owner_user_id', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    sa.Column('agent', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('session_key', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('kind', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    sa.Column('day', sa.Date(), nullable=True),
    sa.Column('created_at', sqlmodel.sql.sqltypes.UTCDateTime(), nullable=False),
    sa.Column('last_turn_at', sqlmodel.sql.sqltypes.UTCDateTime(), nullable=True),
    sa.Column('turn_count', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['client_id', 'home_id'], ['clients.id', 'clients.home_id'], name='fk_conversations_client_home'),
    sa.ForeignKeyConstraint(['home_id'], ['homes.id'], ),
    sa.ForeignKeyConstraint(['owner_user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_conversations_client_id'), 'conversations', ['client_id'], unique=False)
    op.create_index(op.f('ix_conversations_home_id'), 'conversations', ['home_id'], unique=False)
    op.create_index(op.f('ix_conversations_owner_user_id'), 'conversations', ['owner_user_id'], unique=False)
    op.create_index(op.f('ix_conversations_session_key'), 'conversations', ['session_key'], unique=True)
    op.create_index('uq_conversations_default_day', 'conversations', ['client_id', 'agent', 'day'], unique=True)


def downgrade() -> None:
    op.drop_index('uq_conversations_default_day', table_name='conversations')
    op.drop_index(op.f('ix_conversations_session_key'), table_name='conversations')
    op.drop_index(op.f('ix_conversations_owner_user_id'), table_name='conversations')
    op.drop_index(op.f('ix_conversations_home_id'), table_name='conversations')
    op.drop_index(op.f('ix_conversations_client_id'), table_name='conversations')
    op.drop_table('conversations')
    op.drop_table('memberships')
    op.drop_index(op.f('ix_clients_user_id'), table_name='clients')
    op.drop_index(op.f('ix_clients_home_id'), table_name='clients')
    op.drop_index(op.f('ix_clients_key_hash'), table_name='clients')
    op.drop_table('clients')
    op.drop_index(op.f('ix_users_username'), table_name='users')
    op.drop_table('users')
    op.drop_table('homes')
