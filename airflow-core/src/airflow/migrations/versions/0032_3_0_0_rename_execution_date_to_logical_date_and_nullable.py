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
Rename execution_date to logical_date.

The column has been renamed to logical_date, although the Python model is
not changed. This allows us to not need to fix all the Python code at once, but
still do the two changes in one migration instead of two.

Revision ID: 1cdc775ca98f
Revises: a2c32e6c7729
Create Date: 2024-08-28 08:35:26.634475

"""

from __future__ import annotations

from alembic import context, op
from sqlalchemy import text

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = "1cdc775ca98f"
down_revision = "a2c32e6c7729"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column(
            "execution_date",
            new_column_name="logical_date",
            existing_type=TIMESTAMP(timezone=True),
            nullable=True,
        )

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_constraint("dag_run_dag_id_execution_date_key", type_="unique")
        batch_op.create_unique_constraint(
            "dag_run_dag_id_logical_date_key",
            columns=["dag_id", "logical_date"],
        )

    with op.batch_alter_table("log", schema=None) as batch_op:
        batch_op.alter_column(
            "execution_date",
            new_column_name="logical_date",
            existing_type=TIMESTAMP(timezone=True),
            nullable=True,
        )


def _move_offending_dagruns():
    from airflow.utils.db import AIRFLOW_MOVED_TABLE_PREFIX

    select_null_logical_date_query = "select * from dag_run where logical_date is null"

    conn = op.get_bind()
    offline = context.is_offline_mode()

    # If there are no offending rows, we can skip everything.
    # This check is not possible in offline mode.
    if not offline and conn.execute(text(select_null_logical_date_query)).fetchone() is None:
        return

    # Copy offending data to a new table. This can be done directly in Postgres
    # and SQLite with create-from-select; MySQL needs a special case.
    offending_table_name = f"{AIRFLOW_MOVED_TABLE_PREFIX}__3_0_0__offending_dag_run"
    if conn.dialect.name == "mysql":
        op.execute(f"create table {offending_table_name} like dag_run")
        op.execute(f"insert into {offending_table_name} {select_null_logical_date_query}")
    else:
        op.execute(f"create table {offending_table_name} as {select_null_logical_date_query}")

    # In offline mode, since we can't check if there are offending rows, we may
    # end up with an empty offending table. Leave a note for the user to drop it
    # themselves after review.
    if offline:
        op.execute(f"-- TODO: DAG runs unable to be downgraded are moved to {offending_table_name}.")
        op.execute(f"-- TODO: Table {offending_table_name} can be removed after contained data are reviewed.")

    # Remove offending rows so we can continue downgrade.
    op.execute("delete from dag_run where logical_date is null")


def downgrade():
    _move_offending_dagruns()

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column(
            "logical_date",
            new_column_name="execution_date",
            existing_type=TIMESTAMP(timezone=True),
            nullable=False,
        )

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_constraint("dag_run_dag_id_logical_date_key", type_="unique")
        batch_op.create_unique_constraint(
            "dag_run_dag_id_execution_date_key",
            columns=["dag_id", "execution_date"],
        )

    with op.batch_alter_table("log", schema=None) as batch_op:
        batch_op.alter_column(
            "logical_date",
            new_column_name="execution_date",
            existing_type=TIMESTAMP(timezone=True),
            nullable=True,
        )
