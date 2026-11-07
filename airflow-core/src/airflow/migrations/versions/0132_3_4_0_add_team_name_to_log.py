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
Add team_name to log.

Revision ID: 8d3f1a6b2c47
Revises: c7f0a5d2e9b4
Create Date: 2026-08-20 15:02:11.409771

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8d3f1a6b2c47"
down_revision = "c7f0a5d2e9b4"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add team_name to log."""
    op.add_column("log", sa.Column("team_name", sa.String(length=50), nullable=True))
    op.create_index("idx_log_team_name", "log", ["team_name"], unique=False)


def downgrade():
    """Unapply Add team_name to log."""
    op.drop_index("idx_log_team_name", table_name="log")
    op.drop_column("log", "team_name")
