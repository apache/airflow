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
Drop the ti_heartbeat index from task_instance.

Every heartbeat writes ``last_heartbeat_at``; this index stops PostgreSQL from applying those writes as HOT
updates. It serves only the heartbeat timeout check, and only when finished attempts do not outnumber running
ones; in the usual case the planner relies on ``state``.

Revision ID: 1d032b08a6d1
Revises: feb8b1e719ee
Create Date: 2026-11-16 18:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

revision = "1d032b08a6d1"
down_revision = "feb8b1e719ee"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


# Deployment managers may have dropped the index by hand before upgrading, since that only makes heartbeats
# cheaper, or recreated it before downgrading, so both steps accept either state. MySQL has no IF EXISTS / IF NOT
# EXISTS for indexes: there the schema is inspected, and offline (``--show-sql-only``) the SQL is for the schema
# Airflow created.
def _mysql_has_ti_heartbeat(*, offline_default: bool) -> bool:
    if context.is_offline_mode():
        return offline_default
    return "ti_heartbeat" in {
        index["name"] for index in sa.inspect(op.get_bind()).get_indexes("task_instance")
    }


def upgrade():
    """Drop the ti_heartbeat index, unless it is already gone."""
    if op.get_context().dialect.name != "mysql":
        op.drop_index("ti_heartbeat", table_name="task_instance", if_exists=True)
    elif _mysql_has_ti_heartbeat(offline_default=True):
        op.drop_index("ti_heartbeat", table_name="task_instance")


def downgrade():
    """Recreate the ti_heartbeat index, unless it already exists."""
    if op.get_context().dialect.name != "mysql":
        op.create_index(
            "ti_heartbeat", "task_instance", ["last_heartbeat_at"], unique=False, if_not_exists=True
        )
    elif not _mysql_has_ti_heartbeat(offline_default=False):
        op.create_index("ti_heartbeat", "task_instance", ["last_heartbeat_at"], unique=False)
