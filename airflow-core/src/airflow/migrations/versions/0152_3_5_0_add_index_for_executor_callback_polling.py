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
Add index for executor callback polling.

Revision ID: f49cb41e6fd6
Revises: 38c8df3fcb9c
Create Date: 2026-10-30 00:58:13.913853

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f49cb41e6fd6"
down_revision = "38c8df3fcb9c"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add the executor callback polling index."""
    with op.batch_alter_table("callback", schema=None) as batch_op:
        batch_op.create_index(
            "idx_callback_state_type_priority",
            ["state", "type", sa.literal_column("priority_weight DESC")],
            unique=False,
        )


def downgrade():
    """Remove the executor callback polling index."""
    with op.batch_alter_table("callback", schema=None) as batch_op:
        batch_op.drop_index("idx_callback_state_type_priority")
