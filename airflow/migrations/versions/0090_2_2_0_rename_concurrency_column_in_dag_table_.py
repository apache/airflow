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
"""Rename ``concurrency`` column in ``dag`` table to`` max_active_tasks``

Revision ID: 30867afad44a
Revises: e9304a3141f0
Create Date: 2021-06-04 22:11:19.849981

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '30867afad44a'
down_revision = 'e9304a3141f0'
branch_labels = None
depends_on = None
airflow_version = '2.2.0'


def upgrade():
    """Apply Rename ``concurrency`` column in ``dag`` table to`` max_active_tasks``"""
    conn = op.get_bind()
    is_sqlite = bool(conn.dialect.name == "sqlite")

    if is_sqlite:
        op.execute("PRAGMA foreign_keys=off")
    with op.batch_alter_table('dag') as batch_op:
        batch_op.alter_column(
            'concurrency',
            new_column_name='max_active_tasks',
            type_=sa.Integer(),
            nullable=False,
        )
    if is_sqlite:
        op.execute("PRAGMA foreign_keys=on")


def downgrade():
    """Unapply Rename ``concurrency`` column in ``dag`` table to`` max_active_tasks``"""
    with op.batch_alter_table('dag') as batch_op:
        batch_op.alter_column(
            'max_active_tasks',
            new_column_name='concurrency',
            type_=sa.Integer(),
            nullable=False,
        )
