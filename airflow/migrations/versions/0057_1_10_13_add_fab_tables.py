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
"""Create FAB Tables

Revision ID: 92c57b58940d
Revises: da3f683c3a5a
Create Date: 2020-11-13 19:27:10.161814

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.compat.sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = '92c57b58940d'
down_revision = 'da3f683c3a5a'
branch_labels = None
depends_on = None
airflow_version = '1.10.13'


def upgrade():
    """Create FAB Tables"""
    conn = op.get_bind()
    inspector = inspect(conn)
    tables = inspector.get_table_names()
    if "ab_permission" not in tables:
        op.create_table(
            'ab_permission',
            sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
            sa.Column('name', sa.String(length=100), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('name'),
        )

    if "ab_view_menu" not in tables:
        op.create_table(
            'ab_view_menu',
            sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
            sa.Column('name', sa.String(length=100), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('name'),
        )

    if "ab_role" not in tables:
        op.create_table(
            'ab_role',
            sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
            sa.Column('name', sa.String(length=64), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('name'),
        )

    if "ab_permission_view" not in tables:
        op.create_table(
            'ab_permission_view',
            sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
            sa.Column('permission_id', sa.Integer(), nullable=True),
            sa.Column('view_menu_id', sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(['permission_id'], ['ab_permission.id']),
            sa.ForeignKeyConstraint(['view_menu_id'], ['ab_view_menu.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('permission_id', 'view_menu_id'),
        )

    if "ab_permission_view_role" not in tables:
        op.create_table(
            'ab_permission_view_role',
            sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
            sa.Column('permission_view_id', sa.Integer(), nullable=True),
            sa.Column('role_id', sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(['permission_view_id'], ['ab_permission_view.id']),
            sa.ForeignKeyConstraint(['role_id'], ['ab_role.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint("permission_view_id", "role_id"),
        )

    if "ab_user" not in tables:
        op.create_table(
            'ab_user',
            sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
            sa.Column('first_name', sa.String(length=64), nullable=False),
            sa.Column('last_name', sa.String(length=64), nullable=False),
            sa.Column('username', sa.String(length=64), nullable=False),
            sa.Column('password', sa.String(length=256), nullable=True),
            sa.Column('active', sa.Boolean(), nullable=True),
            sa.Column('email', sa.String(length=64), nullable=False),
            sa.Column('last_login', sa.DateTime(), nullable=True),
            sa.Column('login_count', sa.Integer(), nullable=True),
            sa.Column('fail_login_count', sa.Integer(), nullable=True),
            sa.Column('created_on', sa.DateTime(), nullable=True),
            sa.Column('changed_on', sa.DateTime(), nullable=True),
            sa.Column('created_by_fk', sa.Integer(), nullable=True),
            sa.Column('changed_by_fk', sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(['changed_by_fk'], ['ab_user.id']),
            sa.ForeignKeyConstraint(['created_by_fk'], ['ab_user.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('email'),
            sa.UniqueConstraint('username'),
        )

    if "ab_user_role" not in tables:
        op.create_table(
            'ab_user_role',
            sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
            sa.Column('user_id', sa.Integer(), nullable=True),
            sa.Column('role_id', sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(
                ['role_id'],
                ['ab_role.id'],
            ),
            sa.ForeignKeyConstraint(
                ['user_id'],
                ['ab_user.id'],
            ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('user_id', 'role_id'),
        )

    if "ab_register_user" not in tables:
        op.create_table(
            'ab_register_user',
            sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
            sa.Column('first_name', sa.String(length=64), nullable=False),
            sa.Column('last_name', sa.String(length=64), nullable=False),
            sa.Column('username', sa.String(length=64), nullable=False),
            sa.Column('password', sa.String(length=256), nullable=True),
            sa.Column('email', sa.String(length=64), nullable=False),
            sa.Column('registration_date', sa.DateTime(), nullable=True),
            sa.Column('registration_hash', sa.String(length=256), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('username'),
        )


def downgrade():
    """Drop FAB Tables"""
    conn = op.get_bind()
    inspector = inspect(conn)
    tables = inspector.get_table_names()
    fab_tables = [
        "ab_permission",
        "ab_view_menu",
        "ab_role",
        "ab_permission_view",
        "ab_permission_view_role",
        "ab_user",
        "ab_user_role",
        "ab_register_user",
    ]

    for table in fab_tables:
        if table in tables:
            indexes = inspector.get_foreign_keys(table)
            for index in indexes:
                if conn.dialect.name != "sqlite":
                    op.drop_constraint(index.get('name'), table, type_='foreignkey')

    for table in fab_tables:
        if table in tables:
            if conn.dialect.name == "sqlite":
                op.execute("PRAGMA foreign_keys=off")
                op.drop_table(table)
                op.execute("PRAGMA foreign_keys=on")
            else:
                op.drop_table(table)
