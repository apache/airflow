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
Add composite index on task_instance(updated_at, dag_id).

Without this index, GET /dags/~/dagRuns/~/taskInstances filtered by an
updated_at range (e.g. hourly polling windows) plus the dag_id IN (...)
clause injected by PermittedTIFilter causes a full sequential scan on
task_instance, observed at ~39s avg latency in RDS Performance Insights.

Putting updated_at first lets Postgres bound the scan to the time window,
then use dag_id as a secondary filter within that range.

Revision ID: d1e2f3a4b5c6
Revises: 9fabad868fdb
Create Date: 2026-04-20 00:00:00.000000

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "d1e2f3a4b5c6"
down_revision = "9fabad868fdb"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add composite index on task_instance(updated_at, dag_id)."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.create_index(
            "ti_updated_at_dag_id",
            ["updated_at", "dag_id"],
        )


def downgrade():
    """Remove composite index on task_instance(updated_at, dag_id)."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_index("ti_updated_at_dag_id")
