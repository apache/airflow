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
Add run_type to the DagRun (dag_id, logical_date) unique constraint.

Revision ID: 60e5d30563ae
Revises: 8d3f1a6b2c47
Create Date: 2026-08-22 21:15:00.000000

"""

from __future__ import annotations

from alembic import context, op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "60e5d30563ae"
down_revision = "8d3f1a6b2c47"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

_OLD_CONSTRAINT = "dag_run_dag_id_logical_date_key"
_NEW_CONSTRAINT = "dag_run_dag_id_logical_date_run_type_key"

_DUPLICATE_LOGICAL_DATE_QUERY = (
    "select dr.* from dag_run dr "
    "where (dr.dag_id, dr.logical_date) in ("
    "    select dag_id, logical_date from dag_run group by dag_id, logical_date having count(*) > 1"
    ") "
    "and dr.id not in ("
    "    select min(id) from dag_run group by dag_id, logical_date having count(*) > 1"
    ")"
)


def upgrade():
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_constraint(_OLD_CONSTRAINT, type_="unique")
        batch_op.create_unique_constraint(_NEW_CONSTRAINT, columns=["dag_id", "logical_date", "run_type"])


def _move_offending_dagruns():
    from airflow.utils.db import AIRFLOW_MOVED_TABLE_PREFIX

    conn = op.get_bind()
    offline = context.is_offline_mode()

    if not offline and conn.execute(text(_DUPLICATE_LOGICAL_DATE_QUERY)).fetchone() is None:
        return

    offending_table_name = f"{AIRFLOW_MOVED_TABLE_PREFIX}__3_4_0__offending_dag_run_logical_date"
    if conn.dialect.name == "mysql":
        op.execute(f"create table {offending_table_name} like dag_run")
        op.execute(f"insert into {offending_table_name} {_DUPLICATE_LOGICAL_DATE_QUERY}")
    else:
        op.execute(f"create table {offending_table_name} as {_DUPLICATE_LOGICAL_DATE_QUERY}")

    if offline:
        op.execute(f"-- TODO: DAG runs unable to be downgraded are moved to {offending_table_name}.")
        op.execute(f"-- TODO: Table {offending_table_name} can be removed after contained data are reviewed.")

    op.execute(f"delete from dag_run where id in (select id from {offending_table_name})")


def downgrade():
    _move_offending_dagruns()

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_constraint(_NEW_CONSTRAINT, type_="unique")
        batch_op.create_unique_constraint(_OLD_CONSTRAINT, columns=["dag_id", "logical_date"])
