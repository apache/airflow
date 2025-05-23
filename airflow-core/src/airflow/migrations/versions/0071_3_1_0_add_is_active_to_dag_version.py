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
Add soft-delete flag is_active to DagVersion.

Revision ID: 03e36c7f30aa
Revises: 0242ac120002
Create Date: 2025-05-20 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "03e36c7f30aa"
down_revision = "0242ac120002"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Add is_active column to dag_version and backfill to True."""
    op.add_column(
        "dag_version",
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default="1",
            comment="Soft-delete flag; only active versions show up in APIs",
        ),
    )


def downgrade():
    """Remove is_active column from dag_version."""
    op.drop_column("dag_version", "is_active")
