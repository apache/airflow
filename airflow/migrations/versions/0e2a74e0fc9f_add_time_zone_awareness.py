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

"""Add time zone awareness

Revision ID: 0e2a74e0fc9f
Revises: d2ae31099d61
Create Date: 2017-11-10 22:22:31.326152

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "0e2a74e0fc9f"
down_revision = "d2ae31099d61"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        conn.execute("SET time_zone = '+00:00'")
        cur = conn.execute("SELECT @@explicit_defaults_for_timestamp")
        res = cur.fetchall()
        if res[0][0] == 0:
            raise Exception(
                "Global variable explicit_defaults_for_timestamp needs to be on (1) for mysql"
            )

        op.alter_column(
            table_name="chart",
            column_name="last_modified",
            type_=mysql.TIMESTAMP(fsp=6),
        )

        op.alter_column(
            table_name="dag",
            column_name="last_scheduler_run",
            type_=mysql.TIMESTAMP(fsp=6),
        )
        op.alter_column(
            table_name="dag", column_name="last_pickled", type_=mysql.TIMESTAMP(fsp=6)
        )
        op.alter_column(
            table_name="dag", column_name="last_expired", type_=mysql.TIMESTAMP(fsp=6)
        )

        op.alter_column(
            table_name="dag_pickle",
            column_name="created_dttm",
            type_=mysql.TIMESTAMP(fsp=6),
        )

        op.alter_column(
            table_name="dag_run",
            column_name="execution_date",
            type_=mysql.TIMESTAMP(fsp=6),
        )
        op.alter_column(
            table_name="dag_run", column_name="start_date", type_=mysql.TIMESTAMP(fsp=6)
        )
        op.alter_column(
            table_name="dag_run", column_name="end_date", type_=mysql.TIMESTAMP(fsp=6)
        )

        op.alter_column(
            table_name="import_error",
            column_name="timestamp",
            type_=mysql.TIMESTAMP(fsp=6),
        )

        op.alter_column(
            table_name="job", column_name="start_date", type_=mysql.TIMESTAMP(fsp=6)
        )
        op.alter_column(
            table_name="job", column_name="end_date", type_=mysql.TIMESTAMP(fsp=6)
        )
        op.alter_column(
            table_name="job",
            column_name="latest_heartbeat",
            type_=mysql.TIMESTAMP(fsp=6),
        )

        op.alter_column(
            table_name="log", column_name="dttm", type_=mysql.TIMESTAMP(fsp=6)
        )
        op.alter_column(
            table_name="log", column_name="execution_date", type_=mysql.TIMESTAMP(fsp=6)
        )

        op.alter_column(
            table_name="sla_miss",
            column_name="execution_date",
            type_=mysql.TIMESTAMP(fsp=6),
            nullable=False,
        )
        op.alter_column(
            table_name="sla_miss", column_name="timestamp", type_=mysql.TIMESTAMP(fsp=6)
        )

        op.alter_column(
            table_name="task_fail",
            column_name="execution_date",
            type_=mysql.TIMESTAMP(fsp=6),
        )
        op.alter_column(
            table_name="task_fail",
            column_name="start_date",
            type_=mysql.TIMESTAMP(fsp=6),
        )
        op.alter_column(
            table_name="task_fail", column_name="end_date", type_=mysql.TIMESTAMP(fsp=6)
        )

        op.alter_column(
            table_name="task_instance",
            column_name="execution_date",
            type_=mysql.TIMESTAMP(fsp=6),
            nullable=False,
        )
        op.alter_column(
            table_name="task_instance",
            column_name="start_date",
            type_=mysql.TIMESTAMP(fsp=6),
        )
        op.alter_column(
            table_name="task_instance",
            column_name="end_date",
            type_=mysql.TIMESTAMP(fsp=6),
        )
        op.alter_column(
            table_name="task_instance",
            column_name="queued_dttm",
            type_=mysql.TIMESTAMP(fsp=6),
        )

        op.alter_column(
            table_name="xcom", column_name="timestamp", type_=mysql.TIMESTAMP(fsp=6)
        )
        op.alter_column(
            table_name="xcom",
            column_name="execution_date",
            type_=mysql.TIMESTAMP(fsp=6),
        )
    else:
        # sqlite and mssql datetime are fine as is.  Therefore, not converting
        if conn.dialect.name in ("sqlite", "mssql"):
            return

        # we try to be database agnostic, but not every db (e.g. sqlserver)
        # supports per session time zones
        if conn.dialect.name == "postgresql":
            conn.execute("set timezone=UTC")

        op.alter_column(
            table_name="chart",
            column_name="last_modified",
            type_=sa.TIMESTAMP(timezone=True),
        )

        op.alter_column(
            table_name="dag",
            column_name="last_scheduler_run",
            type_=sa.TIMESTAMP(timezone=True),
        )
        op.alter_column(
            table_name="dag",
            column_name="last_pickled",
            type_=sa.TIMESTAMP(timezone=True),
        )
        op.alter_column(
            table_name="dag",
            column_name="last_expired",
            type_=sa.TIMESTAMP(timezone=True),
        )

        op.alter_column(
            table_name="dag_pickle",
            column_name="created_dttm",
            type_=sa.TIMESTAMP(timezone=True),
        )

        op.alter_column(
            table_name="dag_run",
            column_name="execution_date",
            type_=sa.TIMESTAMP(timezone=True),
        )
        op.alter_column(
            table_name="dag_run",
            column_name="start_date",
            type_=sa.TIMESTAMP(timezone=True),
        )
        op.alter_column(
            table_name="dag_run",
            column_name="end_date",
            type_=sa.TIMESTAMP(timezone=True),
        )

        op.alter_column(
            table_name="import_error",
            column_name="timestamp",
            type_=sa.TIMESTAMP(timezone=True),
        )

        op.alter_column(
            table_name="job",
            column_name="start_date",
            type_=sa.TIMESTAMP(timezone=True),
        )
        op.alter_column(
            table_name="job", column_name="end_date", type_=sa.TIMESTAMP(timezone=True)
        )
        op.alter_column(
            table_name="job",
            column_name="latest_heartbeat",
            type_=sa.TIMESTAMP(timezone=True),
        )

        op.alter_column(
            table_name="log", column_name="dttm", type_=sa.TIMESTAMP(timezone=True)
        )
        op.alter_column(
            table_name="log",
            column_name="execution_date",
            type_=sa.TIMESTAMP(timezone=True),
        )

        op.alter_column(
            table_name="sla_miss",
            column_name="execution_date",
            type_=sa.TIMESTAMP(timezone=True),
            nullable=False,
        )
        op.alter_column(
            table_name="sla_miss",
            column_name="timestamp",
            type_=sa.TIMESTAMP(timezone=True),
        )

        op.alter_column(
            table_name="task_fail",
            column_name="execution_date",
            type_=sa.TIMESTAMP(timezone=True),
        )
        op.alter_column(
            table_name="task_fail",
            column_name="start_date",
            type_=sa.TIMESTAMP(timezone=True),
        )
        op.alter_column(
            table_name="task_fail",
            column_name="end_date",
            type_=sa.TIMESTAMP(timezone=True),
        )

        op.alter_column(
            table_name="task_instance",
            column_name="execution_date",
            type_=sa.TIMESTAMP(timezone=True),
            nullable=False,
        )
        op.alter_column(
            table_name="task_instance",
            column_name="start_date",
            type_=sa.TIMESTAMP(timezone=True),
        )
        op.alter_column(
            table_name="task_instance",
            column_name="end_date",
            type_=sa.TIMESTAMP(timezone=True),
        )
        op.alter_column(
            table_name="task_instance",
            column_name="queued_dttm",
            type_=sa.TIMESTAMP(timezone=True),
        )

        op.alter_column(
            table_name="xcom",
            column_name="timestamp",
            type_=sa.TIMESTAMP(timezone=True),
        )
        op.alter_column(
            table_name="xcom",
            column_name="execution_date",
            type_=sa.TIMESTAMP(timezone=True),
        )


def downgrade():
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        conn.execute("SET time_zone = '+00:00'")
        op.alter_column(
            table_name="chart", column_name="last_modified", type_=mysql.DATETIME(fsp=6)
        )

        op.alter_column(
            table_name="dag",
            column_name="last_scheduler_run",
            type_=mysql.DATETIME(fsp=6),
        )
        op.alter_column(
            table_name="dag", column_name="last_pickled", type_=mysql.DATETIME(fsp=6)
        )
        op.alter_column(
            table_name="dag", column_name="last_expired", type_=mysql.DATETIME(fsp=6)
        )

        op.alter_column(
            table_name="dag_pickle",
            column_name="created_dttm",
            type_=mysql.DATETIME(fsp=6),
        )

        op.alter_column(
            table_name="dag_run",
            column_name="execution_date",
            type_=mysql.DATETIME(fsp=6),
        )
        op.alter_column(
            table_name="dag_run", column_name="start_date", type_=mysql.DATETIME(fsp=6)
        )
        op.alter_column(
            table_name="dag_run", column_name="end_date", type_=mysql.DATETIME(fsp=6)
        )

        op.alter_column(
            table_name="import_error",
            column_name="DATETIME",
            type_=mysql.DATETIME(fsp=6),
        )

        op.alter_column(
            table_name="job", column_name="start_date", type_=mysql.DATETIME(fsp=6)
        )
        op.alter_column(
            table_name="job", column_name="end_date", type_=mysql.DATETIME(fsp=6)
        )
        op.alter_column(
            table_name="job",
            column_name="latest_heartbeat",
            type_=mysql.DATETIME(fsp=6),
        )

        op.alter_column(
            table_name="log", column_name="dttm", type_=mysql.DATETIME(fsp=6)
        )
        op.alter_column(
            table_name="log", column_name="execution_date", type_=mysql.DATETIME(fsp=6)
        )

        op.alter_column(
            table_name="sla_miss",
            column_name="execution_date",
            type_=mysql.DATETIME(fsp=6),
            nullable=False,
        )
        op.alter_column(
            table_name="sla_miss", column_name="DATETIME", type_=mysql.DATETIME(fsp=6)
        )

        op.alter_column(
            table_name="task_fail",
            column_name="execution_date",
            type_=mysql.DATETIME(fsp=6),
        )
        op.alter_column(
            table_name="task_fail",
            column_name="start_date",
            type_=mysql.DATETIME(fsp=6),
        )
        op.alter_column(
            table_name="task_fail", column_name="end_date", type_=mysql.DATETIME(fsp=6)
        )

        op.alter_column(
            table_name="task_instance",
            column_name="execution_date",
            type_=mysql.DATETIME(fsp=6),
            nullable=False,
        )
        op.alter_column(
            table_name="task_instance",
            column_name="start_date",
            type_=mysql.DATETIME(fsp=6),
        )
        op.alter_column(
            table_name="task_instance",
            column_name="end_date",
            type_=mysql.DATETIME(fsp=6),
        )
        op.alter_column(
            table_name="task_instance",
            column_name="queued_dttm",
            type_=mysql.DATETIME(fsp=6),
        )

        op.alter_column(
            table_name="xcom", column_name="DATETIME", type_=mysql.DATETIME(fsp=6)
        )
        op.alter_column(
            table_name="xcom", column_name="execution_date", type_=mysql.DATETIME(fsp=6)
        )
    else:
        if conn.dialect.name in ("sqlite", "mssql"):
            return

        # we try to be database agnostic, but not every db (e.g. sqlserver)
        # supports per session time zones
        if conn.dialect.name == "postgresql":
            conn.execute("set timezone=UTC")

        op.alter_column(
            table_name="chart", column_name="last_modified", type_=sa.DateTime()
        )

        op.alter_column(
            table_name="dag", column_name="last_scheduler_run", type_=sa.DateTime()
        )
        op.alter_column(
            table_name="dag", column_name="last_pickled", type_=sa.DateTime()
        )
        op.alter_column(
            table_name="dag", column_name="last_expired", type_=sa.DateTime()
        )

        op.alter_column(
            table_name="dag_pickle", column_name="created_dttm", type_=sa.DateTime()
        )

        op.alter_column(
            table_name="dag_run", column_name="execution_date", type_=sa.DateTime()
        )
        op.alter_column(
            table_name="dag_run", column_name="start_date", type_=sa.DateTime()
        )
        op.alter_column(
            table_name="dag_run", column_name="end_date", type_=sa.DateTime()
        )

        op.alter_column(
            table_name="import_error", column_name="timestamp", type_=sa.DateTime()
        )

        op.alter_column(table_name="job", column_name="start_date", type_=sa.DateTime())
        op.alter_column(table_name="job", column_name="end_date", type_=sa.DateTime())
        op.alter_column(
            table_name="job", column_name="latest_heartbeat", type_=sa.DateTime()
        )

        op.alter_column(table_name="log", column_name="dttm", type_=sa.DateTime())
        op.alter_column(
            table_name="log", column_name="execution_date", type_=sa.DateTime()
        )

        op.alter_column(
            table_name="sla_miss",
            column_name="execution_date",
            type_=sa.DateTime(),
            nullable=False,
        )
        op.alter_column(
            table_name="sla_miss", column_name="timestamp", type_=sa.DateTime()
        )

        op.alter_column(
            table_name="task_fail", column_name="execution_date", type_=sa.DateTime()
        )
        op.alter_column(
            table_name="task_fail", column_name="start_date", type_=sa.DateTime()
        )
        op.alter_column(
            table_name="task_fail", column_name="end_date", type_=sa.DateTime()
        )

        op.alter_column(
            table_name="task_instance",
            column_name="execution_date",
            type_=sa.DateTime(),
            nullable=False,
        )
        op.alter_column(
            table_name="task_instance", column_name="start_date", type_=sa.DateTime()
        )
        op.alter_column(
            table_name="task_instance", column_name="end_date", type_=sa.DateTime()
        )
        op.alter_column(
            table_name="task_instance", column_name="queued_dttm", type_=sa.DateTime()
        )

        op.alter_column(table_name="xcom", column_name="timestamp", type_=sa.DateTime())
        op.alter_column(
            table_name="xcom", column_name="execution_date", type_=sa.DateTime()
        )
