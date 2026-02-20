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
Add dag_run dag_id start_date index.

Revision ID: c40451c900e0
Revises: f8c9d7e6b5a4
Create Date: 2026-02-19 10:18:05.276440

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "c40451c900e0"
down_revision = "f8c9d7e6b5a4"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add composite index on dag_run(dag_id, start_date)."""
    op.create_index("idx_dag_run_dag_id_start_date", "dag_run", ["dag_id", "start_date"])


def downgrade():
    """Remove composite index on dag_run(dag_id, start_date)."""
    op.drop_index("idx_dag_run_dag_id_start_date", table_name="dag_run")
