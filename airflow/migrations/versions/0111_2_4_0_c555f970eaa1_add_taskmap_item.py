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

"""Add item to TaskMap.

Revision ID: c555f970eaa1
Revises: 3c94c427fdf6
Create Date: 2022-06-02 11:55:25.727690
"""

from alembic import op
from sqlalchemy import Column, Integer, MetaData, Table, and_, literal_column, select

from airflow.migrations.utils import disable_sqlite_fkeys
from airflow.models.base import StringID
from airflow.utils.sqlalchemy import ExtendedJSON

# Revision identifiers, used by Alembic.
revision = "c555f970eaa1"
down_revision = "3c94c427fdf6"
branch_labels = None
depends_on = None

airflow_version = "2.4.0"

metadata = MetaData()


def upgrade():
    """Switch TaskMap to use ``dag_run_id``, and add ``item`` to it.

    For performance reasons, this is done by creating a new table with needed
    data pre-populated, renaming it to replace the existing TaskMap table, and
    adding back constraints we need.
    """
    taskmap = Table(
        "task_map",
        metadata,
        Column("dag_id", StringID(), nullable=False),
        Column("run_id", StringID(), nullable=False),
        Column("task_id", StringID(), nullable=False),
        Column("map_index", Integer(), nullable=False),
        Column("length", Integer(), nullable=False),
        Column("keys", ExtendedJSON(), nullable=True),
    )
    dagrun = Table(
        "dag_run",
        metadata,
        Column("id", Integer(), primary_key=True),
        Column("dag_id", StringID(), nullable=False),
        Column("run_id", StringID(), nullable=False),
    )

    op.create_table(
        "__airflow_tmp_task_map",
        Column("dag_run_id", Integer(), nullable=False),
        Column("task_id", StringID(), nullable=False),
        Column("map_index", Integer(), nullable=False),
        Column("item", StringID(), nullable=False),
        Column("dag_id", StringID(), nullable=False),
        Column("run_id", StringID(), nullable=False),
        Column("length", Integer(), nullable=False),
        Column("keys", ExtendedJSON(), nullable=True),
    )
    query = select(
        [
            dagrun.c["id"],
            taskmap.c["task_id"],
            taskmap.c["map_index"],
            literal_column("''"),
            taskmap.c["dag_id"],
            taskmap.c["run_id"],
            taskmap.c["length"],
            taskmap.c["keys"],
        ],
    ).select_from(
        taskmap.join(
            right=dagrun,
            onclause=and_(
                taskmap.c.dag_id == dagrun.c.dag_id,
                taskmap.c.run_id == dagrun.c.run_id,
            ),
        ),
    )
    op.execute(f"INSERT INTO __airflow_tmp_task_map {query.selectable.compile(op.get_bind())}")

    with disable_sqlite_fkeys(op):
        op.drop_table("task_map")
    op.rename_table("__airflow_tmp_task_map", "task_map")

    with op.batch_alter_table("task_map") as batch_op:
        batch_op.create_primary_key("task_map_pkey", ["dag_run_id", "task_id", "map_index", "item"])
        batch_op.create_check_constraint("task_map_length_not_negative", "length >= 0")
        batch_op.create_index("idx_task_map_item", ["item"])
        batch_op.create_foreign_key(
            "task_map_task_instance_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
        )


def downgrade():
    """Remove ``dag_run_id`` and ``item``, and re-create primary key.

    Since we're only removing fields here (and the primary key always needs to
    be created from scratch anyway), we can do this in-place without jumping
    through the same create-rename loops for upgrading.
    """
    with op.batch_alter_table("task_map") as batch_op:
        if op.get_bind().dialect.name != "sqlite":
            batch_op.drop_constraint("task_map_pkey", type_="primary")
        batch_op.execute("DELETE FROM task_map WHERE item != ''")
        batch_op.drop_index("idx_task_map_item")
        batch_op.drop_column("item")
        batch_op.drop_column("dag_run_id")
        batch_op.create_primary_key("task_map_pkey", ["dag_id", "task_id", "run_id", "map_index"])
