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
Add DagRun run_after.

Revision ID: 6a9e7a527a88
Revises: 33b04e4bfa19
Create Date: 2025-01-16 10:07:34.940948
"""

from __future__ import annotations

import alembic.op as op
import sqlalchemy as sa

from airflow.utils.sqlalchemy import UtcDateTime

# Revision identifiers, used by Alembic.
revision = "6a9e7a527a88"
down_revision = "33b04e4bfa19"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Add DagRun run_after."""
    op.add_column("dag_run", sa.Column("run_after", UtcDateTime, nullable=True))

    # run_after must be set to after the data interval ends.
    # In very old runs where the data interval is not available, we'll just use
    # the logical date. This is wrong (one interval too early), but good enough
    # since those super old runs should have their data interval ended a long
    # time ago anyway, and setting
    op.execute("update dag_run set run_after = coalesce(data_interval_end, logical_date)")

    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.alter_column("run_after", existing_type=UtcDateTime, nullable=False)
        batch_op.create_index("idx_dag_run_run_after", ["run_after"], unique=False)


def downgrade():
    """Remove DagRun run_after."""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.drop_index("idx_dag_run_run_after")
        batch_op.drop_column("run_after")
