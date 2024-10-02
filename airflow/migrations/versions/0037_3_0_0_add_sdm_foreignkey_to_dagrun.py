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
Add SDM foreignkey to DagRun.

Revision ID: 4235395d5ec5
Revises: e1ff90d3efe9
Create Date: 2024-10-03 13:37:55.678831

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4235395d5ec5"
down_revision = "e1ff90d3efe9"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Add SDM foreignkey to DagRun."""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.add_column(sa.Column("serialized_dag_id", sa.Integer()))
        batch_op.create_foreign_key(
            "dag_run_serialized_dag_fkey",
            "serialized_dag",
            ["serialized_dag_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.drop_column("dag_hash")

    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.add_column(sa.Column("serialized_dag_id", sa.Integer()))
        batch_op.create_foreign_key(
            "task_instance_serialized_dag_fkey",
            "serialized_dag",
            ["serialized_dag_id"],
            ["id"],
            ondelete="SET NULL",
        )

    with op.batch_alter_table("task_instance_history") as batch_op:
        batch_op.add_column(sa.Column("serialized_dag_id", sa.Integer()))


def downgrade():
    """Unapply Add SDM foreignkey to DagRun."""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.add_column(sa.Column("dag_hash", sa.String(32)))
        batch_op.drop_constraint("dag_run_serialized_dag_fkey", type_="foreignkey")
        batch_op.drop_column("serialized_dag_id")

    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_constraint("task_instance_serialized_dag_fkey", type_="foreignkey")
        batch_op.drop_column("serialized_dag_id")

    with op.batch_alter_table("task_instance_history") as batch_op:
        batch_op.drop_column("serialized_dag_id")
