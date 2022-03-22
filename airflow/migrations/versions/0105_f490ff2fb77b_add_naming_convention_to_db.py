#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""add naming convention to the DB

Revision ID: f490ff2fb77b
Revises: 4eaab2fe6582
Create Date: 2022-03-16 23:09:04.329259

"""
from alembic import op
from sqlalchemy import inspect

from airflow.migrations.utils import exclude_table, get_mssql_table_constraints

# revision identifiers, used by Alembic.
revision = 'f490ff2fb77b'
down_revision = '4eaab2fe6582'
branch_labels = None
depends_on = None
airflow_version = '2.3.0'

TABLE_NAME_UNIQUE_KEY_MAP = {'slot_pool': ['pool'], 'variable': ['key'], 'connection': ['conn_id']}


def drop_and_create_pkey(inspector, table_name, fk_table_name, fk_table_fk_name, fk_kwargs=None):
    if not fk_kwargs:
        fk_kwargs = {}
    fks_ab_view_role = inspector.get_foreign_keys(fk_table_name)
    for fk in fks_ab_view_role:
        if fk['referred_table'] == table_name:
            op.drop_constraint(fk['name'], fk_table_name, type_='foreignkey')
    pk = inspector.get_pk_constraint(table_name)
    op.drop_constraint(pk['name'], table_name, type_='primary')
    op.create_primary_key(f'{table_name}_pkey', table_name, pk['constrained_columns'])
    for fk in fks_ab_view_role:
        if fk['referred_table'] == table_name:
            op.create_foreign_key(
                fk_table_fk_name,
                fk_table_name,
                referent_table=fk['referred_table'],
                local_cols=fk['constrained_columns'],
                remote_cols=fk['referred_columns'],
                **fk_kwargs,
            )


def drop_and_create_taskinstance_pkey(inspector):
    table_name = 'task_instance'
    op.drop_constraint('rtif_ti_fkey', 'rendered_task_instance_fields', type_='foreignkey')
    op.drop_constraint('task_map_task_instance_fkey', 'task_map', type_='foreignkey')
    op.drop_constraint('task_reschedule_ti_fkey', 'task_reschedule', type_='foreignkey')
    op.drop_constraint('xcom_task_instance_fkey', 'xcom', type_='foreignkey')
    # drop and recreate the primary key
    pk = inspector.get_pk_constraint(table_name)
    op.drop_constraint(pk['name'], table_name, type_='primary')
    op.create_primary_key(f'{table_name}_pkey', table_name, pk['constrained_columns'])
    op.create_foreign_key(
        'rtif_ti_fkey',
        'rendered_task_instance_fields',
        'task_instance',
        ['dag_id', 'task_id', 'run_id', 'map_index'],
        ['dag_id', 'task_id', 'run_id', 'map_index'],
        ondelete='CASCADE',
    )
    op.create_foreign_key(
        "task_map_task_instance_fkey",
        "task_map",
        "task_instance",
        ["dag_id", "task_id", "run_id", "map_index"],
        ['dag_id', 'task_id', 'run_id', 'map_index'],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        'task_reschedule_ti_fkey',
        'task_reschedule',
        'task_instance',
        ["dag_id", "task_id", "run_id", "map_index"],
        ["dag_id", "task_id", "run_id", "map_index"],
        ondelete='CASCADE',
    )
    op.create_foreign_key(
        'xcom_task_instance_fkey',
        'xcom',
        'task_instance',
        ["dag_id", "task_id", "run_id", "map_index"],
        ["dag_id", "task_id", "run_id", "map_index"],
        ondelete='CASCADE',
    )


def drop_and_create_ab_role_pkey(inspector):
    table_name = 'ab_role'
    fks_user_role = inspector.get_foreign_keys('ab_user_role')
    for fk in fks_user_role:
        if fk['referred_table'] == table_name:
            op.drop_constraint(fk['name'], 'ab_user_role', type_='foreignkey')
    fks_perms = inspector.get_foreign_keys('ab_permission_view_role')
    for fk in fks_perms:
        if fk['referred_table'] == table_name:
            op.drop_constraint(fk['name'], 'ab_permission_view_role', type_='foreignkey')
    pk = inspector.get_pk_constraint(table_name)
    op.drop_constraint(pk['name'], table_name, type_='primary')
    op.create_primary_key(f'{table_name}_pkey', table_name, pk['constrained_columns'])
    for fk in fks_user_role:
        if fk['referred_table'] == table_name:
            op.create_foreign_key(
                'ab_user_role_role_id_fkey',
                'ab_user_role',
                referent_table=fk['referred_table'],
                local_cols=fk['constrained_columns'],
                remote_cols=fk['referred_columns'],
            )
    for fk in fks_perms:
        if fk['referred_table'] == table_name:
            op.create_foreign_key(
                'ab_permission_view_role_role_id_fkey',
                'ab_permission_view_role',
                referent_table=fk['referred_table'],
                local_cols=fk['constrained_columns'],
                remote_cols=fk['referred_columns'],
            )


def drop_and_create_ab_user_pkey(inspector):
    table_name = 'ab_user'
    # drop ab_user_role foreign key
    fk_user_role = inspector.get_foreign_keys('ab_user_role')
    for fk in fk_user_role:
        if fk['referred_table'] == 'ab_user':
            op.drop_constraint(fk['name'], 'ab_user_role', type_='foreignkey')
    # drop ab_user created_by foreign key
    fks = inspector.get_foreign_keys('ab_user')
    for fk in fks:
        if fk['constrained_columns'][0] in ['created_by_fk', 'changed_by_fk']:
            op.drop_constraint(fk['name'], 'ab_user', type_='foreignkey')

    pk = inspector.get_pk_constraint(table_name)
    op.drop_constraint(pk['name'], table_name, type_='primary')
    op.create_primary_key(f'{table_name}_pkey', table_name, pk['constrained_columns'])

    for fk in fk_user_role:
        if fk['referred_table'] == 'ab_user':
            op.create_foreign_key(
                'ab_user_role_user_id_ab_user_fkey',
                'ab_user_role',
                referent_table=fk['referred_table'],
                local_cols=fk['constrained_columns'],
                remote_cols=fk['referred_columns'],
            )

    for fk in fks:
        if fk['referred_columns'] == ['created_by_fk']:
            op.create_foreign_key(
                'ab_user_created_by_fk_ab_user_fkey',
                'ab_user',
                referent_table=fk['referred_table'],
                local_cols=fk['constrained_columns'],
                remote_cols=fk['referred_columns'],
            )
        elif fk['referred_columns'] == ['changed_by_fk']:
            op.create_foreign_key(
                'ab_user_changed_by_fk_ab_user_fkey',
                'ab_user',
                referent_table=fk['referred_table'],
                local_cols=fk['constrained_columns'],
                remote_cols=fk['referred_columns'],
            )


def upgrade():
    """Apply add naming convention to db"""
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    if dialect_name == 'mssql':
        # unnamed Primary keys, uniques and foreign keys are automatically named
        # by MSSQL. Here we give all the names explicitly following the naming convention
        # at airflow/models/base.py
        # For more information about what the tokens in the convention mean, see
        # https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData.params.naming_convention
        insp = inspect(conn)
        tables = insp.get_table_names()
        for table_name in tables:
            if exclude_table(table_name) or table_name == 'alembic_version':
                continue
            if table_name == 'task_instance':
                drop_and_create_taskinstance_pkey(insp)
            elif table_name == 'log_template':
                drop_and_create_pkey(
                    insp,
                    table_name,
                    'dag_run',
                    'task_instance_log_template_id_fkey',
                    fk_kwargs={"ondelete": "NO ACTION"},
                )
            elif table_name == 'trigger':
                drop_and_create_pkey(
                    insp,
                    table_name,
                    'task_instance',
                    'task_instance_trigger_id_fkey',
                    fk_kwargs={'onupdate': 'CASCADE'},
                )
            elif table_name == 'dag':
                drop_and_create_pkey(insp, table_name, 'dag_tag', 'dag_tag_dag_id_fkey')
            elif table_name == 'ab_role':
                drop_and_create_ab_role_pkey(insp)
            elif table_name == 'ab_user':
                drop_and_create_ab_user_pkey(insp)
            elif table_name == 'ab_permission_view':
                drop_and_create_pkey(insp, table_name, 'ab_permission_view_role', 'ab_pvr_perms_view_id_fkey')
            elif table_name == 'ab_view_menu':
                drop_and_create_pkey(insp, table_name, 'ab_permission_view', 'ab_pv_view_menu_id_fkey')
            elif table_name == 'ab_permission':
                drop_and_create_pkey(insp, table_name, 'ab_permission_view', 'ab_pv_permission_id_fkey')
            else:
                # drop and recreate the primary key
                pk = insp.get_pk_constraint(table_name)
                op.drop_constraint(pk['name'], table_name, type_='primary')
                op.create_primary_key(f'{table_name}_pkey', table_name, pk['constrained_columns'])

        for table_name, key in TABLE_NAME_UNIQUE_KEY_MAP.items():
            constraints = get_mssql_table_constraints(conn, table_name)
            uk, _ = constraints['UNIQUE'].popitem()
            op.drop_constraint(uk, table_name, type_='unique')
            joined_keys = '_'.join(key)
            op.create_unique_constraint(f'{table_name}_{joined_keys}_key', table_name, key)

    else:
        with op.batch_alter_table('connection', schema=None) as batch_op:
            batch_op.drop_constraint('unique_conn_id', type_='unique')
            batch_op.create_unique_constraint(batch_op.f('connection_conn_id_key'), ['conn_id'])


def downgrade():
    """Unapply add naming convention to db"""
    # We don't need to drop the constraints updated above for MSSQL
    # MSSQL constraints are always randomized so it'll still not make a difference
    with op.batch_alter_table('connection', schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f('connection_conn_id_key'), type_='unique')
        batch_op.create_unique_constraint('unique_conn_id', ['conn_id'])
