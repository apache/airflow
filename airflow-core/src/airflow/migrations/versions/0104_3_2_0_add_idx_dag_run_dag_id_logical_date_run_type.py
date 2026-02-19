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
Add index for dag_run, logical_date, run_type to DagRun.

Revision ID: 7cb5fa064991
Revises: f8c9d7e6b5a4
Create Date: 2026-02-18 16:32:17.494524

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "7cb5fa064991"
down_revision = "f8c9d7e6b5a4"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add index for dag_run, logical_date, run_type to DagRun."""
    op.create_index(
        "idx_dag_run_dag_id_logical_date_run_type",
        "dag_run",
        ["dag_id", "logical_date", "run_type"],
        unique=False,
    )


def downgrade():
    """Revert index for dag_run, logical_date, run_type on DagRun."""
    op.drop_index("idx_dag_run_dag_id_logical_date_run_type", table_name="dag_run")
