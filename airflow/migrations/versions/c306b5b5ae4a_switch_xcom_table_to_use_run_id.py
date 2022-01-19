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

"""Switch XCom table to use ``run_id``.

Revision ID: c306b5b5ae4a
Revises: a3bcd0914482
Create Date: 2022-01-19 03:20:35.329037
"""
from alembic import op
from sqlalchemy import Column

from airflow.migrations.db_types import TIMESTAMP, StringID

# Revision identifiers, used by Alembic.
revision = "c306b5b5ae4a"
down_revision = "a3bcd0914482"
branch_labels = None
depends_on = None


def upgrade():
    """Switch XCom table to use run_id.

    For performance reasons, this is done by creating a new table with needed
    data pre-populated, adding back constraints we need, and renaming it to
    replace the existing XCom table.
    """
    if op.get_bind().dialect.name == "mysql":
        # CREATE TABLE ... AS SELECT does not work well when MySQL replication
        # is enabled, so we do this in multiple steps instead.
        op.execute("CREATE TABLE __airflow_tmp_xcom LIKE xcom")
        with op.batch_alter_table("__airflow_tmp_xcom") as batch_op:
            batch_op.add_column(Column("run_id", StringID()))
            batch_op.drop_column("execution_date")
        op.execute(
            """
            INSERT INTO __airflow_tmp_xcom
            SELECT x.key, x.value, x.timestamp, x.task_id, x.dag_id, r.run_id
            FROM xcom AS x
            INNER JOIN dag_run AS r
            ON r.dag_id = x.dag_id
            AND r.execution_date = x.execution_date
            """,
        )
    else:
        op.execute(
            """
            CREATE TABLE __airflow_tmp_xcom
            AS SELECT x.key, x.value, x.timestamp, x.task_id, x.dag_id, r.run_id
            FROM xcom AS x
            INNER JOIN dag_run AS r
            ON r.dag_id = x.dag_id
            AND r.execution_date = x.execution_date
            """,
        )

    with op.batch_alter_table("__airflow_tmp_xcom") as batch_op:
        batch_op.alter_column("timestamp", existing_type=StringID(), nullable=False)
        batch_op.create_primary_key("xcom_pkey", ["key", "dag_id", "task_id", "run_id"])

    op.drop_table("xcom")
    op.rename_table("__airflow_tmp_xcom", "xcom")


def downgrade():
    """Switch XCom table back to use execution_date.

    Basically an inverse operation.
    """
    if op.get_bind().dialect.name == "mysql":
        # CREATE TABLE ... AS SELECT does not work well when MySQL replication
        # is enabled, so we do this in multiple steps instead.
        op.execute("CREATE TABLE __airflow_tmp_xcom LIKE xcom")
        with op.batch_alter_table("__airflow_tmp_xcom") as batch_op:
            batch_op.add_column(Column("execution_date", TIMESTAMP))
            batch_op.drop_column("run_id")
        op.execute(
            """
            INSERT INTO __airflow_tmp_xcom
            SELECT x.key, x.value, x.timestamp, x.task_id, x.dag_id, r.execution_date
            FROM xcom AS x
            INNER JOIN dag_run AS r
            ON r.dag_id = x.dag_id
            AND r.run_id = x.run_id
            """,
        )
    else:
        op.execute(
            """
            CREATE TABLE __airflow_tmp_xcom
            AS SELECT x.key, x.value, x.timestamp, x.task_id, x.dag_id, r.execution_date
            FROM xcom AS x
            INNER JOIN dag_run AS r
            ON r.dag_id = x.dag_id
            AND r.run_id = x.run_id
            """,
        )

    with op.batch_alter_table("__airflow_tmp_xcom") as batch_op:
        batch_op.alter_column("timestamp", existing_type=StringID(), nullable=False)
        batch_op.create_primary_key("xcom_pkey", ["key", "dag_id", "task_id", "execution_date"])

    op.drop_table("xcom")
    op.rename_table("__airflow_tmp_xcom", "xcom")
