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
Add owner_display_name to (Audit) Log table.

Revision ID: f7bf2a57d0a6
Revises: 375a816bbbf4
Create Date: 2023-09-12 17:21:45.149658

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f7bf2a57d0a6"
down_revision = "375a816bbbf4"
branch_labels = None
depends_on = None
airflow_version = "2.8.0"

TABLE_NAME = "log"


def upgrade():
    """Add owner_display_name column to log."""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.add_column(sa.Column("owner_display_name", sa.String(500)))


def downgrade():
    """Remove owner_display_name column from log."""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.drop_column("owner_display_name")
