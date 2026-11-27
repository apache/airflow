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
Link executor callbacks to their Dag runs.

Revision ID: b54a81cd79e2
Revises: f9520b8eed39
Create Date: 2026-11-25
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import sqlite_rebuilds

revision = "b54a81cd79e2"
down_revision = "f9520b8eed39"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    with sqlite_rebuilds(op), op.batch_alter_table("callback") as batch_op:
        batch_op.add_column(sa.Column("dag_run_id", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("has_dag_run_reference", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("heartbeat_supported", sa.Boolean(), nullable=True))
        batch_op.create_foreign_key(
            batch_op.f("callback_dag_run_id_fkey"), "dag_run", ["dag_run_id"], ["id"], ondelete="SET NULL"
        )
        batch_op.create_index("idx_callback_dag_run_id", ["dag_run_id"])


def downgrade():
    with sqlite_rebuilds(op), op.batch_alter_table("callback") as batch_op:
        batch_op.drop_constraint(batch_op.f("callback_dag_run_id_fkey"), type_="foreignkey")
        batch_op.drop_index("idx_callback_dag_run_id")
        batch_op.drop_column("heartbeat_supported")
        batch_op.drop_column("has_dag_run_reference")
        batch_op.drop_column("dag_run_id")
