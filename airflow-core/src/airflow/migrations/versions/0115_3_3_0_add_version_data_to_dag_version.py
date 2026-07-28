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
Add version_data to dag_version.

Adds a nullable JSON column to the dag_version table for storing
structured version metadata (e.g., S3 object version manifests).
Bundles that do not use version_data leave this column NULL.

On PostgreSQL and MySQL 8+, adding a nullable column without a
default is a metadata-only operation (no table rewrite).

Revision ID: a1b2c3d4e5f6
Revises: a7f3b2c1d4e5
Create Date: 2026-05-05 23:30:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "a1b2c3d4e5f6"
down_revision = "a7f3b2c1d4e5"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add version_data column to dag_version table."""
    op.add_column("dag_version", sa.Column("version_data", sa.JSON(), nullable=True))


def downgrade():
    """Remove version_data column from dag_version table."""
    op.drop_column("dag_version", "version_data")
