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
Add TaskMap and map_index on TaskInstance.

Add ``map_index`` column to TaskInstance to identify task-mapping,
and a ``task_map`` table to track mapping values from XCom.

Revision ID: e655c0453f75
Revises: f9da662e7089
Create Date: 2021-12-13 22:59:41.052584
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import CheckConstraint, Column, ForeignKeyConstraint, Integer, text

from airflow.models.base import StringID
from airflow.utils.sqlalchemy import ExtendedJSON

# Revision identifiers, used by Alembic.
revision = "e655c0453f75"
down_revision = "f9da662e7089"
branch_labels = None
depends_on = None
airflow_version = "2.3.0"


def upgrade():
    """Add TaskMap and map_index on TaskInstance."""
    # We need to first remove constraints on task_reschedule since they depend on task_instance.
    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
        batch_op.drop_index("idx_task_reschedule_dag_task_run")

    # Change task_instance's primary key.
    with op.batch_alter_table("task_instance") as batch_op:
        # I think we always use this name for TaskInstance after 7b2661a43ba3?
        batch_op.drop_constraint("task_instance_pkey", type_="primary")
        batch_op.add_column(Column("map_index", Integer, nullable=False, server_default=text("-1")))
        batch_op.create_primary_key("task_instance_pkey", ["dag_id", "task_id", "run_id", "map_index"])

    # Re-create task_reschedule's constraints.
    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.add_column(Column("map_index", Integer, nullable=False, server_default=text("-1")))
        batch_op.create_foreign_key(
            "task_reschedule_ti_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
        )
        batch_op.create_index(
            "idx_task_reschedule_dag_task_run",
            ["dag_id", "task_id", "run_id", "map_index"],
            unique=False,
        )

    # Create task_map.
    op.create_table(
        "task_map",
        Column("dag_id", StringID(), primary_key=True),
        Column("task_id", StringID(), primary_key=True),
        Column("run_id", StringID(), primary_key=True),
        Column("map_index", Integer, primary_key=True),
        Column("length", Integer, nullable=False),
        Column("keys", ExtendedJSON, nullable=True),
        CheckConstraint("length >= 0", name="task_map_length_not_negative"),
        ForeignKeyConstraint(
            ["dag_id", "task_id", "run_id", "map_index"],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_map_task_instance_fkey",
            ondelete="CASCADE",
        ),
    )


def downgrade():
    """Remove TaskMap and map_index on TaskInstance."""
    op.drop_table("task_map")

    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
        batch_op.drop_index("idx_task_reschedule_dag_task_run")
        batch_op.drop_column("map_index", mssql_drop_default=True)

    op.execute("DELETE FROM task_instance WHERE map_index != -1")

    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_constraint("task_instance_pkey", type_="primary")
        batch_op.drop_column("map_index", mssql_drop_default=True)
        batch_op.create_primary_key("task_instance_pkey", ["dag_id", "task_id", "run_id"])

    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.create_foreign_key(
            "task_reschedule_ti_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id"],
            ["dag_id", "task_id", "run_id"],
            ondelete="CASCADE",
        )
        batch_op.create_index(
            "idx_task_reschedule_dag_task_run",
            ["dag_id", "task_id", "run_id"],
            unique=False,
        )
