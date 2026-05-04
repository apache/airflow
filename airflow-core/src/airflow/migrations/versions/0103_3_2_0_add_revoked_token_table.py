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
Add revoked_token table.

Revision ID: 53ff648b8a26
Revises: a5a3e5eb9b8d
Create Date: 2026-02-01 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "53ff648b8a26"
down_revision = "a5a3e5eb9b8d"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add revoked_token table."""
    op.create_table(
        "revoked_token",
        sa.Column("jti", sa.String(32), primary_key=True, nullable=False),
        sa.Column("exp", UtcDateTime, nullable=False, index=True),
    )


def downgrade():
    """Drop revoked_token table."""
    op.drop_table("revoked_token")
