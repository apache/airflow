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
Add partial index on task_instance (state, updated_at) for terminal states.

Revision ID: c4f5e6d7a8b9
Revises: 9fabad868fdb
Create Date: 2026-04-20 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c4f5e6d7a8b9"
down_revision = "9fabad868fdb"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add partial index on task_instance (state, updated_at) for terminal states."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.create_index(
            "ti_state_updated_at",
            ["state", "updated_at"],
            postgresql_where=sa.text("state IN ('success', 'failed')"),
            sqlite_where=sa.text("state IN ('success', 'failed')"),
        )


def downgrade():
    """Remove partial index on task_instance (state, updated_at) for terminal states."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_index("ti_state_updated_at")
