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
    dialect = op.get_bind().dialect.name
    if dialect == "mysql":
        # CREATE TABLE ... AS SELECT does not work well when MySQL replication
        # is enabled, so we do this in multiple steps instead.
        op.execute("CREATE TABLE __airflow_tmp_xcom LIKE xcom")
        op.drop_constraint("xcom_pkey", "__airflow_tmp_xcom", type_="primary")
        op.add_column("__airflow_tmp_xcom", Column("run_id", StringID()))
        op.drop_column("__airflow_tmp_xcom", "execution_date")
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
    elif dialect == "mssql":
        # MSSQL's syntax is SELECT INTO FROM.
        op.create(
            """
            SELECT x.key, x.value, x.timestamp, x.task_id, x.dag_id, r.run_id
            INTO __airflow_tmp_xcom
            FROM xcom AS x
            INNER JOIN dag_run AS r
            ON r.dag_id = x.dag_id
            AND r.execution_date = x.execution_date
            """,
        )
    else:
        # This works for both Postgres and SQLite.
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
        batch_op.alter_column("timestamp", existing_type=TIMESTAMP, nullable=False)
        if dialect == "mssql":
            batch_op.alter_column("key", existing_type=StringID(length=512), nullable=False)
            batch_op.alter_column("task_id", existing_type=StringID(), nullable=False)
            batch_op.alter_column("dag_id", existing_type=StringID(), nullable=False)
            batch_op.alter_column("run_id", existing_type=StringID(), nullable=False)

    op.create_primary_key("xcom_pkey", "__airflow_tmp_xcom", ["key", "dag_id", "task_id", "run_id"])

    op.drop_table("xcom")
    op.rename_table("__airflow_tmp_xcom", "xcom")


def downgrade():
    """Switch XCom table back to use execution_date.

    Basically an inverse operation.
    """
    dialect = op.get_bind().dialect.name
    if dialect == "mysql":
        # CREATE TABLE ... AS SELECT does not work well when MySQL replication
        # is enabled, so we do this in multiple steps instead.
        op.execute("CREATE TABLE __airflow_tmp_xcom LIKE xcom")
        op.add_column("__airflow_tmp_xcom", Column("execution_date", TIMESTAMP, nullable=False))
        op.drop_column("__airflow_tmp_xcom", "run_id")
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
    elif dialect == "mssql":
        op.create(
            """
            SELECT x.key, x.value, x.timestamp, x.task_id, x.dag_id, r.execution_date
            INTO __airflow_tmp_xcom
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
        batch_op.alter_column("timestamp", existing_type=TIMESTAMP, nullable=False)
        if dialect == "mssql":
            batch_op.alter_column("key", existing_type=StringID(length=512), nullable=False)
            batch_op.alter_column("task_id", existing_type=StringID(), nullable=False)
            batch_op.alter_column("dag_id", existing_type=StringID(), nullable=False)
            batch_op.alter_column("execution_date", existing_type=TIMESTAMP, nullable=False)

    op.create_primary_key("xcom_pkey", "__airflow_tmp_xcom", ["key", "dag_id", "task_id", "execution_date"])

    op.drop_table("xcom")
    op.rename_table("__airflow_tmp_xcom", "xcom")
