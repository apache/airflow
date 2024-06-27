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
Add ``max_active_runs`` column to ``dag_model`` table.

Revision ID: 092435bf5d12
Revises: 97cdd93827b8
Create Date: 2021-09-06 21:29:24.728923

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "092435bf5d12"
down_revision = "97cdd93827b8"
branch_labels = None
depends_on = None
airflow_version = "2.1.4"


def upgrade():
    """Apply Add ``max_active_runs`` column to ``dag_model`` table."""
    op.add_column("dag", sa.Column("max_active_runs", sa.Integer(), nullable=True))
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        # Add index to dag_run.dag_id and also add index to dag_run.state where state==running
        batch_op.create_index("idx_dag_run_dag_id", ["dag_id"])
        batch_op.create_index(
            "idx_dag_run_running_dags",
            ["state", "dag_id"],
            postgresql_where=text("state='running'"),
            mssql_where=text("state='running'"),
            sqlite_where=text("state='running'"),
        )


def downgrade():
    """Unapply Add ``max_active_runs`` column to ``dag_model`` table."""
    with op.batch_alter_table("dag") as batch_op:
        batch_op.drop_column("max_active_runs")
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        # Drop index to dag_run.dag_id and also drop index to dag_run.state where state==running
        batch_op.drop_index("idx_dag_run_dag_id")
        batch_op.drop_index("idx_dag_run_running_dags")
