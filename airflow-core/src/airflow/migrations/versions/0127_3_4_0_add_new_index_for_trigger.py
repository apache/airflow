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
Add new index for trigger.

Revision ID: 76c46545c91e
Revises: c4e7a1f9b2d0
Create Date: 2026-07-13 11:06:57.281704

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "76c46545c91e"
down_revision = "c4e7a1f9b2d0"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add new index for trigger."""
    with op.batch_alter_table("trigger", schema=None) as batch_op:
        batch_op.create_index("idx_trigger_triggerer_queue_id", ["triggerer_id", "queue", "id"], unique=False)


def downgrade():
    """Remove new index for trigger."""
    with op.batch_alter_table("trigger", schema=None) as batch_op:
        batch_op.drop_index("idx_trigger_triggerer_queue_id")
