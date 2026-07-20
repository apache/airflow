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
Add ti_state_active_composite index to task_instance.

Revision ID: c7e1f40d6e24
Revises: c4e7a1f9b2d0
Create Date: 2026-07-17 18:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "c7e1f40d6e24"
down_revision = "c4e7a1f9b2d0"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add ti_state_active_composite index to task_instance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.create_index(
            "ti_state_active_composite",
            ["state", "dag_id", "task_id", "run_id"],
            unique=False,
        )


def downgrade():
    """Drop ti_state_active_composite index from task_instance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_index("ti_state_active_composite")
