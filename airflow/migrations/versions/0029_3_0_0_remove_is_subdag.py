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


def upgrade():
    """Remove ``is_subdag`` column from DAGs table."""
    with op.batch_alter_table("dag") as batch_op:
        batch_op.drop_column("is_subdag")
        batch_op.drop_index("idx_root_dag_id")
        batch_op.drop_column("root_dag_id")


def downgrade():
    """Add ``is_subdag`` column in DAGs table."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("is_subdag", sa.BOOLEAN(), nullable=True))
        batch_op.add_column(sa.Column("root_dag_id", StringID(), nullable=True))
        batch_op.create_index("idx_root_dag_id", ["root_dag_id"], unique=False)
