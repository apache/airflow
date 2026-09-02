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
Add draining state to DagModel.

Revision ID: b6a9c2e7d410
Revises: f8c2a1d94e03
Create Date: 2026-09-01 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "b6a9c2e7d410"
down_revision = "f8c2a1d94e03"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add the Dag draining state."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.add_column(sa.Column("is_draining", sa.Boolean(), nullable=False, server_default="0"))
            batch_op.create_index("idx_dag_is_draining", ["is_draining"], unique=False)
            batch_op.create_check_constraint(
                "dag_pause_state_valid",
                "NOT (is_paused AND is_draining)",
            )


def downgrade():
    """Remove the Dag draining state."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.drop_constraint("dag_pause_state_valid", type_="check")
            batch_op.drop_index("idx_dag_is_draining")
            batch_op.drop_column("is_draining")
