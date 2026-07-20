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
Add fire_on_failure to deadline_alert.

Revision ID: b4f8c2a1d9e0
Revises: c4e7a1f9b2d0
Create Date: 2026-07-01 12:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "b4f8c2a1d9e0"
down_revision = "c4e7a1f9b2d0"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add fire_on_failure column to deadline_alert."""
    with op.batch_alter_table("deadline_alert", schema=None) as batch_op:
        batch_op.add_column(sa.Column("fire_on_failure", sa.Boolean(), nullable=False, server_default="0"))


def downgrade():
    """Remove fire_on_failure column from deadline_alert."""
    with op.batch_alter_table("deadline_alert", schema=None) as batch_op:
        batch_op.drop_column("fire_on_failure")
