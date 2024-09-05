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

"""
Remove SubDAGs: ``is_subdag`` & ``root_dag_id`` columns from DAG table.

Revision ID: d0f1c55954fa
Revises: 044f740568ec
Create Date: 2024-08-11 21:32:40.576172

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "d0f1c55954fa"
down_revision = "044f740568ec"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def _column_exists(inspector, column_name):
    return column_name in [col["name"] for col in inspector.get_columns("dag")]


def _index_exists(inspector, index_name):
    return index_name in [index["name"] for index in inspector.get_indexes("dag")]


def upgrade():
    """Remove ``is_subdag`` column from DAGs table."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    with op.batch_alter_table("dag", schema=None) as batch_op:
        if _index_exists(inspector, "idx_root_dag_id"):
            batch_op.drop_index("idx_root_dag_id")
        if _column_exists(inspector, "is_subdag"):
            batch_op.drop_column("is_subdag")
        if _column_exists(inspector, "root_dag_id"):
            batch_op.drop_column("root_dag_id")


def downgrade():
    """Add ``is_subdag`` column in DAGs table."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    with op.batch_alter_table("dag", schema=None) as batch_op:
        if not _column_exists(inspector, "is_subdag"):
            batch_op.add_column(sa.Column("is_subdag", sa.BOOLEAN(), nullable=True))
        if not _column_exists(inspector, "root_dag_id"):
            batch_op.add_column(sa.Column("root_dag_id", StringID(), nullable=True))
        if not _index_exists(inspector, "idx_root_dag_id"):
            batch_op.create_index("idx_root_dag_id", ["root_dag_id"], unique=False)
