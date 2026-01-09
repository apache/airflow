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
Add callback_state to deadline.

Revision ID: f56f68b9e02f
Revises: 09fa89ba1710
Create Date: 2025-07-22 17:46:40.122517

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f56f68b9e02f"
down_revision = "09fa89ba1710"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Add callback_state to deadline."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("callback_state", sa.String(length=20), nullable=True))
        batch_op.drop_index(batch_op.f("deadline_time_idx"))
        batch_op.create_index(
            "deadline_callback_state_time_idx", ["callback_state", "deadline_time"], unique=False
        )


def downgrade():
    """Remove callback_state from deadline."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_index("deadline_callback_state_time_idx")
        batch_op.create_index(batch_op.f("deadline_time_idx"), ["deadline_time"], unique=False)
        batch_op.drop_column("callback_state")
