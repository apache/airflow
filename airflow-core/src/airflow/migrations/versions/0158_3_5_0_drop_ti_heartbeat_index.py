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

Revision ID: 3f1c9a7d2b54
Revises: c64b10719069
Create Date: 2026-11-22 09:00:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "3f1c9a7d2b54"
down_revision = "c64b10719069"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Drop ti_heartbeat so a heartbeat update no longer writes an entry into every index."""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_index("ti_heartbeat")


def downgrade():
    """Recreate the ti_heartbeat index."""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.create_index("ti_heartbeat", ["last_heartbeat_at"], unique=False)
