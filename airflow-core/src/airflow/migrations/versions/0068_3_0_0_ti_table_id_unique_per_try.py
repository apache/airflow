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
Change TI table to have unique UUID id/pk per attempt.

Revision ID: 29ce7909c52b
Revises: 959e216a3abb
Create Date: 2025-04-09 10:09:53.130924

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils import UUIDType

# revision identifiers, used by Alembic.
revision = "29ce7909c52b"
down_revision = "959e216a3abb"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def _get_uuid_type(dialect_name: str) -> sa.types.TypeEngine:
    if dialect_name != "postgres":
        return sa.String(36)
    return UUIDType(binary=False)


def upgrade():
    """Apply Change TI table to have unique UUID id/pk per attempt."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.create_index("idx_tih_dag_run", ["dag_id", "run_id"], unique=False)
        batch_op.drop_column("task_instance_id")
        batch_op.alter_column(
            "try_id",
            new_column_name="task_instance_id",
            existing_type=_get_uuid_type(dialect_name),
            existing_nullable=False,
        )

    with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
        batch_op.drop_constraint("task_instance_note_ti_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "task_instance_note_ti_fkey",
            "task_instance",
            ["ti_id"],
            ["id"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        )

    # We decided to not migrate/correct the data for task_reschedule as we decided the time to do it wasn't
    # worth it, as 90%+ of the data in the table is not needed -- this table only has use when a Sensor task,
    # with reschedule mode, is in the `running` or `up_for_reschedule` states.
    #
    # Going forward, Airflow will delete rows from this table when the TI is recorded in the history table
    # (i.e. when it's cleared or retired) so the only case in which this data will be accessed and give an
    # incorrect figure is when all of these hold true
    #
    # - a Sensor in reschedule mode
    # - in running or up_for_reschedule states
    # - On a try_number > 1
    # - with a timeout set
    #
    # If all of these are true, then the total runtime will be mistakenly calculated as the start of the first
    # try to now, rather than the start of the current try. But this is such an unlikely set of circumstances
    # that it's not worth the time cost of migrating it.
    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.drop_column("try_number")


def downgrade():
    """Unapply Change TI table to have unique UUID id/pk per attempt."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("try_number", sa.INTEGER(), autoincrement=False, nullable=False, server_default="1")
        )

    with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
        batch_op.drop_constraint("task_instance_note_ti_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "task_instance_note_ti_fkey", "task_instance", ["ti_id"], ["id"], ondelete="CASCADE"
        )

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.alter_column(
            "task_instance_id",
            new_column_name="try_id",
            existing_type=_get_uuid_type(dialect_name),
            existing_nullable=False,
        )
        batch_op.drop_index("idx_tih_dag_run")
    # This has to be in a separate batch, else on sqlite it throws `sqlalchemy.exc.CircularDependencyError`
    # (and on non sqlite batching isn't "a thing", it issue alter tables fine)
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("task_instance_id", UUIDType(binary=False), autoincrement=False, nullable=True)
        )
