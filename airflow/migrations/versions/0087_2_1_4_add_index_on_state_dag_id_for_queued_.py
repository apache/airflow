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
"""Add index on state, dag_id for queued ``dagrun``

Revision ID: ccde3e26fe78
Revises: 092435bf5d12
Create Date: 2021-09-08 16:35:34.867711

"""
from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "ccde3e26fe78"
down_revision = "092435bf5d12"
branch_labels = None
depends_on = None
airflow_version = "2.1.4"


def upgrade():
    """Apply Add index on state, dag_id for queued ``dagrun``"""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.create_index(
            "idx_dag_run_queued_dags",
            ["state", "dag_id"],
            postgresql_where=text("state='queued'"),
            mssql_where=text("state='queued'"),
            sqlite_where=text("state='queued'"),
        )


def downgrade():
    """Unapply Add index on state, dag_id for queued ``dagrun``"""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.drop_index("idx_dag_run_queued_dags")
