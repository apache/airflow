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
Fold task_map into xcom.mapped_length.

``task_map.keys`` is dropped rather than migrated, so downgrade restores every
map as the list variant.

Revision ID: 3b7a91c5df20
Revises: 5182d0596ee2
Create Date: 2026-09-10 10:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.migrations.utils import disable_sqlite_fkeys
from airflow.utils.sqlalchemy import ExtendedJSON

# revision identifiers, used by Alembic.
revision = "3b7a91c5df20"
down_revision = "5182d0596ee2"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

XCOM_RETURN_KEY = "return_value"


def _tables(xcom_name: str, task_map_name: str):
    xcom = sa.table(
        xcom_name,
        sa.column("dag_id"),
        sa.column("task_id"),
        sa.column("run_id"),
        sa.column("map_index"),
        sa.column("key"),
        sa.column("mapped_length"),
    )
    task_map = sa.table(
        task_map_name,
        sa.column("dag_id"),
        sa.column("task_id"),
        sa.column("run_id"),
        sa.column("map_index"),
        sa.column("length"),
        sa.column("keys"),
    )
    join = sa.and_(
        task_map.c.dag_id == xcom.c.dag_id,
        task_map.c.task_id == xcom.c.task_id,
        task_map.c.run_id == xcom.c.run_id,
        task_map.c.map_index == xcom.c.map_index,
    )
    return xcom, task_map, join


def build_backfill_statement(xcom_name: str = "xcom", task_map_name: str = "task_map"):
    """
    Copy each task_map length onto the XCom row it describes.

    A correlated subquery rather than UPDATE ... FROM: MySQL has no such form and SQLite only
    gained it in 3.33.
    """
    xcom, task_map, join = _tables(xcom_name, task_map_name)
    return (
        xcom.update()
        .where(
            xcom.c.key == XCOM_RETURN_KEY,
            sa.exists(sa.select(sa.literal(1)).where(join)),
        )
        .values(mapped_length=sa.select(task_map.c.length).where(join).scalar_subquery())
    )


def build_restore_statement(xcom_name: str = "xcom", task_map_name: str = "task_map"):
    """
    Rebuild task_map rows from the lengths on XCom rows.

    Scoped to one key because task_map's primary key has no key column.
    """
    xcom, task_map, _ = _tables(xcom_name, task_map_name)
    return task_map.insert().from_select(
        ["dag_id", "task_id", "run_id", "map_index", "length", "keys"],
        sa.select(
            xcom.c.dag_id,
            xcom.c.task_id,
            xcom.c.run_id,
            xcom.c.map_index,
            xcom.c.mapped_length,
            sa.null(),
        ).where(xcom.c.mapped_length.is_not(None), xcom.c.key == XCOM_RETURN_KEY),
    )


def upgrade():
    """Fold task_map into xcom.mapped_length."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("xcom", schema=None) as batch_op:
            batch_op.add_column(sa.Column("mapped_length", sa.Integer(), nullable=True))
            batch_op.create_check_constraint("mapped_length_not_negative", "mapped_length >= 0")

        op.execute(build_backfill_statement())
        op.drop_table("task_map")


def downgrade():
    """Restore the task_map table from xcom.mapped_length."""
    with disable_sqlite_fkeys(op):
        op.create_table(
            "task_map",
            sa.Column("dag_id", StringID(length=250), nullable=False),
            sa.Column("task_id", StringID(length=250), nullable=False),
            sa.Column("run_id", StringID(length=250), nullable=False),
            sa.Column("map_index", sa.Integer(), nullable=False),
            sa.Column("length", sa.Integer(), nullable=False),
            sa.Column("keys", ExtendedJSON(), nullable=True),
            sa.CheckConstraint("length >= 0", name="task_map_length_not_negative"),
            sa.ForeignKeyConstraint(
                ["dag_id", "task_id", "run_id", "map_index"],
                [
                    "task_instance.dag_id",
                    "task_instance.task_id",
                    "task_instance.run_id",
                    "task_instance.map_index",
                ],
                name="task_map_task_instance_fkey",
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("dag_id", "task_id", "run_id", "map_index", name="task_map_pkey"),
        )

        op.execute(build_restore_statement())

        with op.batch_alter_table("xcom", schema=None) as batch_op:
            batch_op.drop_constraint("mapped_length_not_negative", type_="check")
            batch_op.drop_column("mapped_length")
