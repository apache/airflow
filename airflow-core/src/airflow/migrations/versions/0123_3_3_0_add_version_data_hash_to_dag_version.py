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
Add version_data_hash column to dag_version table.

Persisting a hash of version_data avoids loading and comparing the full
JSON blob on every DAG parse, keeping DB-side parsing cheap and
memory-flat as version_data grows (large manifests from S3/custom bundles).

Revision ID: 9e8d7c6b5a4f
Revises: 9ff64e1c35d3
Create Date: 2026-06-16 12:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "9e8d7c6b5a4f"
down_revision = "9ff64e1c35d3"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add version_data_hash column to dag_version."""
    with op.batch_alter_table("dag_version", schema=None) as batch_op:
        batch_op.add_column(sa.Column("version_data_hash", sa.String(32), nullable=True))


def downgrade():
    """Remove version_data_hash column from dag_version."""
    from airflow.migrations.utils import disable_sqlite_fkeys

    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("dag_version", schema=None) as batch_op:
            batch_op.drop_column("version_data_hash")
