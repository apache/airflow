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
Store callback deadlines and index waiting and expired callbacks.

Revision ID: f9520b8eed39
Revises: feb8b1e719ee
Create Date: 2026-10-23 09:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

revision = "f9520b8eed39"
down_revision = "feb8b1e719ee"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Existing callbacks have no scheduler deadline until a worker opts in at admission."""
    op.add_column("callback", sa.Column("started_at", UtcDateTime(), nullable=True))
    op.add_column("callback", sa.Column("state_deadline", UtcDateTime(), nullable=True))
    op.create_index(
        "idx_callback_type_state_fetch_method", "callback", ["type", "state", "fetch_method", "id"]
    )
    op.create_index(
        "idx_callback_state_deadline",
        "callback",
        ["state", "state_deadline"],
        postgresql_where=sa.text("state_deadline IS NOT NULL"),
        sqlite_where=sa.text("state_deadline IS NOT NULL"),
    )


def downgrade():
    """Drop the index, and settle routed callbacks, which earlier versions neither route nor reap."""
    callback = sa.table(
        "callback",
        sa.column("type", sa.String),
        sa.column("fetch_method", sa.String),
        sa.column("state", sa.String),
        sa.column("output", sa.Text),
    )
    routed = sa.and_(callback.c.type == "executor", callback.c.fetch_method == "dag_attribute")
    # Not started: hand them back to the Dag processor, which runs them in earlier versions. Their
    # tokens can't start them then, since ``/run`` starts only queued callbacks.
    op.execute(
        callback.update()
        .where(routed, callback.c.state.in_(["scheduled", "queued"]))
        .values(type="dag_processor", state=None)
    )
    op.execute(
        callback.update()
        .where(routed, callback.c.state == "running")
        .values(state="failed", output="Airflow was downgraded before the callback finished")
    )
    op.drop_index("idx_callback_type_state_fetch_method", table_name="callback")
    op.drop_index("idx_callback_state_deadline", table_name="callback")
    op.drop_column("callback", "state_deadline")
    op.drop_column("callback", "started_at")
