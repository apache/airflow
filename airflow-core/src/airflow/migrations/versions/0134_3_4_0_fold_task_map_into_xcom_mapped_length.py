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

Every task_map row is written by the same execution API call that writes the
pushing task's ``return_value`` XCom row, at the same coordinates, so the length
lives on that row instead. ``task_map.keys`` is dropped rather than migrated: the
only writer has always set it to NULL, so downgrade restores every map as the
list variant.

Revision ID: 3b7a91c5df20
Revises: f8c2a1d94e03
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
down_revision = "f8c2a1d94e03"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

XCOM_RETURN_KEY = "return_value"

_xcom = sa.table(
    "xcom",
    sa.column("dag_id"),
    sa.column("task_id"),
    sa.column("run_id"),
    sa.column("map_index"),
    sa.column("key"),
    sa.column("mapped_length"),
)
_task_map = sa.table(
    "task_map",
    sa.column("dag_id"),
    sa.column("task_id"),
    sa.column("run_id"),
    sa.column("map_index"),
    sa.column("length"),
    sa.column("keys"),
)

_JOIN = sa.and_(
    _task_map.c.dag_id == _xcom.c.dag_id,
    _task_map.c.task_id == _xcom.c.task_id,
    _task_map.c.run_id == _xcom.c.run_id,
    _task_map.c.map_index == _xcom.c.map_index,
)

# A correlated subquery rather than UPDATE ... FROM: MySQL has no such form and SQLite only
# gained it in 3.33. The EXISTS keeps the statement a no-op for rows with no task_map row.
BACKFILL = (
    _xcom.update()
    .where(
        _xcom.c.key == XCOM_RETURN_KEY,
        sa.exists(sa.select(sa.literal(1)).where(_JOIN)),
    )
    .values(mapped_length=sa.select(_task_map.c.length).where(_JOIN).scalar_subquery())
)

RESTORE = _task_map.insert().from_select(
    ["dag_id", "task_id", "run_id", "map_index", "length", "keys"],
    sa.select(
        _xcom.c.dag_id,
        _xcom.c.task_id,
        _xcom.c.run_id,
        _xcom.c.map_index,
        _xcom.c.mapped_length,
        sa.null(),
    ).where(_xcom.c.mapped_length.is_not(None)),
)


def upgrade():
    """Fold task_map into xcom.mapped_length."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("xcom", schema=None) as batch_op:
            batch_op.add_column(sa.Column("mapped_length", sa.Integer(), nullable=True))
            batch_op.create_check_constraint("mapped_length_not_negative", "mapped_length >= 0")

        op.execute(BACKFILL)
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

        op.execute(RESTORE)

        with op.batch_alter_table("xcom", schema=None) as batch_op:
            batch_op.drop_constraint("mapped_length_not_negative", type_="check")
            batch_op.drop_column("mapped_length")
