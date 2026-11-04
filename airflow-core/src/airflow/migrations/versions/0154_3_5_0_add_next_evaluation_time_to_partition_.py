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
Add next evaluation time to partition Dag runs.

Revision ID: 03f981e9dae5
Revises: 16d7301f124b
Create Date: 2026-11-04 14:34:56.305007

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys
from airflow.utils.sqlalchemy import UtcDateTime

revision = "03f981e9dae5"
down_revision = "16d7301f124b"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Initialize partition evaluation order from the existing enqueue order."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            batch_op.add_column(sa.Column("next_evaluation_at", UtcDateTime, nullable=True))
        op.execute("UPDATE asset_partition_dag_run SET next_evaluation_at = created_at")
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            batch_op.alter_column("next_evaluation_at", existing_type=UtcDateTime, nullable=False)
            batch_op.create_index(
                "idx_apdr_pending_evaluation",
                ["created_dag_run_id", "next_evaluation_at", "id"],
                unique=False,
            )


def downgrade():
    """Remove the partition evaluation timestamp and index."""
    with disable_sqlite_fkeys(op):
        conn = op.get_bind()
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            if conn.dialect.name == "mysql":
                # MySQL may reuse this index for the Dag run foreign key.
                batch_op.drop_constraint(batch_op.f("apdr_created_dag_run_id_fkey"), type_="foreignkey")
                batch_op.drop_index("idx_apdr_pending_evaluation")
                batch_op.create_foreign_key(
                    batch_op.f("apdr_created_dag_run_id_fkey"),
                    "dag_run",
                    ["created_dag_run_id"],
                    ["id"],
                    ondelete="CASCADE",
                )
            else:
                batch_op.drop_index("idx_apdr_pending_evaluation")
            batch_op.drop_column("next_evaluation_at")
