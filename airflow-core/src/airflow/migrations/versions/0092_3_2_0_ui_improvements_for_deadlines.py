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
Add required fields to enable UI integrations for the Deadline Alerts feature.

Revision ID: 55297ae24532
Revises: b87d2135fa50
Create Date: 2025-10-17 16:04:55.016272
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils import UUIDType

from airflow.migrations.db_types import TIMESTAMP

revision = "55297ae24532"
down_revision = "b87d2135fa50"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Make changes to enable adding DeadlineAlerts to the UI."""
    op.add_column(
        "deadline",
        sa.Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.add_column(
        "deadline",
        sa.Column("last_updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    # Create new deadline_alert table
    op.create_table(
        "deadline_alert",
        sa.Column("id", UUIDType(binary=False)),
        sa.Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("display_name", sa.String(250), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("deadline_alert_pkey")),
    )


def downgrade():
    """Remove changes that were added to enable adding DeadlineAlerts to the UI."""
    op.drop_column("deadline", "last_updated_at", if_exists=True)
    op.drop_column("deadline", "created_at", if_exists=True)

    op.drop_table("deadline_alert")
