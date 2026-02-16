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
Add dag_bundle_refresh_request table.

Revision ID: a1b2c3d4e5f6
Revises: f8c9d7e6b5a4
Create Date: 2026-02-16 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "f8c9d7e6b5a4"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Create dag_bundle_refresh_request table."""
    op.create_table(
        "dag_bundle_refresh_request",
        sa.Column("bundle_name", sa.String(250), primary_key=True, nullable=False),
        sa.Column("created_at", UtcDateTime(), nullable=False),
    )


def downgrade():
    """Drop dag_bundle_refresh_request table."""
    op.drop_table("dag_bundle_refresh_request")
