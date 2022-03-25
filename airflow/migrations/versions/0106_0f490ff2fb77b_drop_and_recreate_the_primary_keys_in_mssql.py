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

"""drop and recreate the primary keys in mssql

Revision ID: f490ff2fb77b
Revises: 909884dea523
Create Date: 2022-03-16 23:09:04.329259

"""
from alembic import op
from alembic.operations.ops import CreateForeignKeyOp, CreatePrimaryKeyOp
from sqlalchemy import MetaData, Table

from airflow.migrations.utils import exclude_table
from airflow.models.base import naming_convention

# revision identifiers, used by Alembic.
revision = 'f490ff2fb77b'
down_revision = '909884dea523'
branch_labels = None
depends_on = None
airflow_version = '2.3.0'


def _recreate_primary_key(table):
    pk = table.primary_key
    op.drop_constraint(pk.name, table.name, type_='primary')
    pk.name = None
    op.invoke(CreatePrimaryKeyOp.from_constraint(pk))


def drop_and_create_pkey(table, meta, conn, fk_table_name):
    t = Table(fk_table_name, meta, autoload_with=conn)
    fks_ab_view_role = t.foreign_key_constraints
    for fk in fks_ab_view_role:
        op.drop_constraint(fk.name, fk_table_name, type_='foreignkey')
    _recreate_primary_key(table)
    for fk in fks_ab_view_role:
        op.invoke(CreateForeignKeyOp.from_constraint(fk))


def drop_and_create_taskinstance_pkey(table):
    op.drop_constraint('rtif_ti_fkey', 'rendered_task_instance_fields', type_='foreignkey')
    op.drop_constraint('task_map_task_instance_fkey', 'task_map', type_='foreignkey')
    op.drop_constraint('task_reschedule_ti_fkey', 'task_reschedule', type_='foreignkey')
    op.drop_constraint('xcom_task_instance_fkey', 'xcom', type_='foreignkey')
    # drop and recreate the primary key
    _recreate_primary_key(table)
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


def drop_and_create_ab_role_pkey(table, meta, conn):
    t = Table('ab_user_role', meta, autoload_with=conn)
    fks_user_role = t.foreign_key_constraints
    for fk in fks_user_role:
        op.drop_constraint(fk.name, 'ab_user_role', type_='foreignkey')
    t = Table('ab_permission_view_role', meta, autoload_with=conn)
    fks_perms = t.foreign_key_constraints
    for fk in fks_perms:
        op.drop_constraint(fk.name, 'ab_permission_view_role', type_='foreignkey')
    _recreate_primary_key(table)
    for fk in fks_user_role:
        op.invoke(CreateForeignKeyOp.from_constraint(fk))
    for fk in fks_perms:
        op.invoke(CreateForeignKeyOp.from_constraint(fk))


def drop_and_create_ab_user_pkey(table, meta, conn):
    # drop ab_user_role foreign key
    t = Table('ab_user_role', meta, autoload_with=conn)
    fk_user_role = t.foreign_key_constraints
    for fk in fk_user_role:
        op.drop_constraint(fk.name, 'ab_user_role', type_='foreignkey')
    # drop ab_user created_by foreign key
    fks = table.foreign_key_constraints
    for fk in fks:
        op.drop_constraint(fk.name, 'ab_user', type_='foreignkey')
    _recreate_primary_key(table)
    for fk in fk_user_role:
        op.invoke(CreateForeignKeyOp.from_constraint(fk))
    for fk in fks:
        op.invoke(CreateForeignKeyOp.from_constraint(fk))


def upgrade():
    """Apply drop and recreate the primary keys in mssql"""
    conn = op.get_bind()
    meta = MetaData(naming_convention=naming_convention)
    engine = conn.engine
    if conn.dialect.name == 'mssql':
        # unnamed Primary keys are automatically named
        # by MSSQL. Here we give all the names explicitly following the naming convention
        # at airflow/models/base.py
        # For more information about what the tokens in the convention mean, see
        # https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData.params.naming_convention

        for table_name in engine.table_names():

            if exclude_table(table_name) or table_name == 'alembic_version':
                continue
            t = Table(table_name, meta, autoload_with=conn)
            if table_name == 'task_instance':
                drop_and_create_taskinstance_pkey(t)
            elif table_name == 'log_template':
                drop_and_create_pkey(
                    t,
                    meta,
                    conn,
                    'dag_run',
                )
            elif table_name == 'trigger':
                drop_and_create_pkey(
                    t,
                    meta,
                    conn,
                    'task_instance',
                )
            elif table_name == 'dag':
                drop_and_create_pkey(t, meta, conn, 'dag_tag')
            elif table_name == 'ab_role':
                drop_and_create_ab_role_pkey(t, meta, conn)
            elif table_name == 'ab_user':
                drop_and_create_ab_user_pkey(t, meta, conn)
            elif table_name == 'ab_permission_view':
                drop_and_create_pkey(t, meta, conn, 'ab_permission_view_role')
            elif table_name == 'ab_view_menu':
                drop_and_create_pkey(t, meta, conn, 'ab_permission_view')
            elif table_name == 'ab_permission':
                drop_and_create_pkey(t, meta, conn, 'ab_permission_view')
            else:
                # drop and recreate the primary key
                _recreate_primary_key(t)


def downgrade():
    """Unapply drop and recreate the primary keys in mssql"""
    conn = op.get_bind()
    meta = MetaData()
    engine = conn.engine
    if conn.dialect.name == 'mssql':
        # unnamed Primary keys are automatically named
        # by MSSQL. Here we give all the names explicitly following the naming convention
        # at airflow/models/base.py
        # For more information about what the tokens in the convention mean, see
        # https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData.params.naming_convention

        for table_name in engine.table_names():

            if exclude_table(table_name) or table_name == 'alembic_version':
                continue
            t = Table(table_name, meta, autoload_with=conn)
            if table_name == 'task_instance':

                drop_and_create_taskinstance_pkey(t)
            elif table_name == 'log_template':
                drop_and_create_pkey(
                    t,
                    meta,
                    conn,
                    'dag_run',
                )
            elif table_name == 'trigger':
                drop_and_create_pkey(
                    t,
                    meta,
                    conn,
                    'task_instance',
                )
            elif table_name == 'dag':
                drop_and_create_pkey(t, meta, conn, 'dag_tag')
            elif table_name == 'ab_role':
                drop_and_create_ab_role_pkey(t, meta, conn)
            elif table_name == 'ab_user':
                drop_and_create_ab_user_pkey(t, meta, conn)
            elif table_name == 'ab_permission_view':
                drop_and_create_pkey(t, meta, conn, 'ab_permission_view_role')
            elif table_name == 'ab_view_menu':
                drop_and_create_pkey(t, meta, conn, 'ab_permission_view')
            elif table_name == 'ab_permission':
                drop_and_create_pkey(t, meta, conn, 'ab_permission_view')
            else:
                # drop and recreate the primary key
                _recreate_primary_key(t)
