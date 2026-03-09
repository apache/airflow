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
Add indexes to DagRun start_date and end_date columns.

Revision ID: 88486830c5c5
Revises: 6222ce48e289
Create Date: 2026-03-08 00:00:00.000000

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "88486830c5c5"
down_revision = "6222ce48e289"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Apply Add indexes to DagRun start_date and end_date columns."""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.create_index("idx_dag_run_start_date", ["start_date"], unique=False)
        batch_op.create_index("idx_dag_run_end_date", ["end_date"], unique=False)


def downgrade():
    """Unapply Add indexes to DagRun start_date and end_date columns."""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.drop_index("idx_dag_run_start_date")
        batch_op.drop_index("idx_dag_run_end_date")
