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

"""Add user comment to task_instance and dag_run.

Revision ID: 65a852f26899
Revises: ecb43d2a1842
Create Date: 2022-09-17 20:01:42.652862

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "65a852f26899"
down_revision = "ee8d93fcc81e"
branch_labels = None
depends_on = None
airflow_version = "2.5.0"


def upgrade():
    """Apply add user comment to task_instance and dag_run"""
    conn = op.get_bind()

    with op.batch_alter_table("dag_run") as batch_op:
        if conn.dialect.name == "mysql":
            batch_op.add_column(sa.Column("notes", sa.Text(length=1000), nullable=True))
        else:
            batch_op.add_column(sa.Column("notes", sa.String(length=1000), nullable=True))

    with op.batch_alter_table("task_instance") as batch_op:
        if conn.dialect.name == "mysql":
            batch_op.add_column(sa.Column("notes", sa.Text(length=1000), nullable=True))
        else:
            batch_op.add_column(sa.Column("notes", sa.String(length=1000), nullable=True))


def downgrade():
    """Unapply add user comment to task_instance and dag_run"""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_column("notes")

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("notes")
