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
"""Make ``TaskInstance.pool`` not nullable

Revision ID: 6e96a59344a4
Revises: 939bb1e647c8
Create Date: 2019-06-13 21:51:32.878437

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "6e96a59344a4"
down_revision = "939bb1e647c8"
branch_labels = None
depends_on = None
airflow_version = "1.10.4"

Base = declarative_base()
ID_LEN = 250


class TaskInstance(Base):  # type: ignore
    """Minimal model definition for migrations"""

    __tablename__ = "task_instance"

    task_id = Column(String(), primary_key=True)
    dag_id = Column(String(), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    pool = Column(String(50), nullable=False)


def upgrade():
    """Make TaskInstance.pool field not nullable."""
    with create_session() as session:
        session.query(TaskInstance).filter(TaskInstance.pool.is_(None)).update(
            {TaskInstance.pool: "default_pool"}, synchronize_session=False
        )  # Avoid select updated rows
        session.commit()

    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        op.drop_index("ti_pool", table_name="task_instance")

    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.alter_column(
            column_name="pool",
            type_=sa.String(50),
            nullable=False,
        )

    if conn.dialect.name == "mssql":
        op.create_index("ti_pool", "task_instance", ["pool", "state", "priority_weight"])


def downgrade():
    """Make TaskInstance.pool field nullable."""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        op.drop_index("ti_pool", table_name="task_instance")

    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.alter_column(
            column_name="pool",
            type_=sa.String(50),
            nullable=True,
        )

    if conn.dialect.name == "mssql":
        op.create_index("ti_pool", "task_instance", ["pool", "state", "priority_weight"])

    with create_session() as session:
        session.query(TaskInstance).filter(TaskInstance.pool == "default_pool").update(
            {TaskInstance.pool: None}, synchronize_session=False
        )  # Avoid select updated rows
        session.commit()
