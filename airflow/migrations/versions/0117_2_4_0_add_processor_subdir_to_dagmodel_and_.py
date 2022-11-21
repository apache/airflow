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
"""Add processor_subdir column to DagModel, SerializedDagModel and CallbackRequest tables.

Revision ID: ecb43d2a1842
Revises: 1486deb605b4
Create Date: 2022-08-26 11:30:11.249580

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ecb43d2a1842"
down_revision = "1486deb605b4"
branch_labels = None
depends_on = None
airflow_version = "2.4.0"


def upgrade():
    """Apply add processor_subdir to DagModel and SerializedDagModel"""
    conn = op.get_bind()

    with op.batch_alter_table("dag") as batch_op:
        if conn.dialect.name == "mysql":
            batch_op.add_column(sa.Column("processor_subdir", sa.Text(length=2000), nullable=True))
        else:
            batch_op.add_column(sa.Column("processor_subdir", sa.String(length=2000), nullable=True))

    with op.batch_alter_table("serialized_dag") as batch_op:
        if conn.dialect.name == "mysql":
            batch_op.add_column(sa.Column("processor_subdir", sa.Text(length=2000), nullable=True))
        else:
            batch_op.add_column(sa.Column("processor_subdir", sa.String(length=2000), nullable=True))

    with op.batch_alter_table("callback_request") as batch_op:
        batch_op.drop_column("dag_directory")
        if conn.dialect.name == "mysql":
            batch_op.add_column(sa.Column("processor_subdir", sa.Text(length=2000), nullable=True))
        else:
            batch_op.add_column(sa.Column("processor_subdir", sa.String(length=2000), nullable=True))


def downgrade():
    """Unapply Add processor_subdir to DagModel and SerializedDagModel"""
    conn = op.get_bind()
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("processor_subdir")

    with op.batch_alter_table("serialized_dag", schema=None) as batch_op:
        batch_op.drop_column("processor_subdir")

    with op.batch_alter_table("callback_request") as batch_op:
        batch_op.drop_column("processor_subdir")
        if conn.dialect.name == "mysql":
            batch_op.add_column(sa.Column("dag_directory", sa.Text(length=1000), nullable=True))
        else:
            batch_op.add_column(sa.Column("dag_directory", sa.String(length=1000), nullable=True))
