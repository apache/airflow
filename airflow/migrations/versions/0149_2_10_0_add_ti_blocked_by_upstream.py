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
Add ti.blocked_by_upstream.

Revision ID: 9e9b810236c1
Revises: ec3471c1e067
Create Date: 2024-07-01 15:53:31.012706

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9e9b810236c1"
down_revision = "ec3471c1e067"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Apply Add ti.blocked_by_upstream."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.add_column(sa.Column("blocked_by_upstream", sa.Boolean(), nullable=True))

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(sa.Column("blocked_by_upstream", sa.Boolean(), nullable=True))


def downgrade():
    """Unapply Add ti.blocked_by_upstream."""
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.drop_column("blocked_by_upstream")

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_column("blocked_by_upstream")
