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

"""Add data_interval_[start|end] to DagModel and DagRun.

Revision ID: 142555e44c17
Revises: 54bebd308c5f
Create Date: 2021-06-09 08:28:02.089817

"""

from alembic import op
from sqlalchemy import TIMESTAMP, Column
from sqlalchemy.dialects import mssql, mysql

# Revision identifiers, used by Alembic.
revision = "142555e44c17"
down_revision = "54bebd308c5f"
branch_labels = None
depends_on = None


def _use_date_time2(conn):
    result = conn.execute(
        """SELECT CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
        like '8%' THEN '2000' WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
        like '9%' THEN '2005' ELSE '2005Plus' END AS MajorVersion"""
    ).fetchone()
    mssql_version = result[0]
    return mssql_version not in ("2000", "2005")


def _get_timestamp(conn):
    dialect_name = conn.dialect.name
    if dialect_name == "mysql":
        return mysql.TIMESTAMP(fsp=6, timezone=True)
    if dialect_name != "mssql":
        return TIMESTAMP(timezone=True)
    if _use_date_time2(conn):
        return mssql.DATETIME2(precision=6)
    return mssql.DATETIME


def upgrade():
    """Apply data_interval fields to DagModel and DagRun."""
    column_type = _get_timestamp(op.get_bind())
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.add_column(Column("data_interval_start", column_type))
        batch_op.add_column(Column("data_interval_end", column_type))
    with op.batch_alter_table("dag") as batch_op:
        batch_op.add_column(Column("next_dagrun_data_interval_start", column_type))
        batch_op.add_column(Column("next_dagrun_data_interval_end", column_type))


def downgrade():
    """Unapply data_interval fields to DagModel and DagRun."""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.drop_column("data_interval_start")
        batch_op.drop_column("data_interval_end")
    with op.batch_alter_table("dag") as batch_op:
        batch_op.drop_column("next_dagrun_data_interval_start")
        batch_op.drop_column("next_dagrun_data_interval_end")
