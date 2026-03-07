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
Set bundle_name non-nullable for legacy DAGs upgraded from 2.x.

Revision ID: 35ab6b577738
Revises: 6222ce48e289
Create Date: 2026-03-05 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "35ab6b577738"
down_revision = "6222ce48e289"
branch_labels = None
depends_on = None

airflow_version = "3.2.0"


def upgrade():
    """Set bundle_name to 'dags-folder' for legacy DAGs with NULL bundle_name and make column non-nullable."""
    op.execute("UPDATE dag SET bundle_name = 'dags-folder' WHERE bundle_name IS NULL")
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column(
            "bundle_name",
            existing_type=sa.String(length=200),
            nullable=False,
        )


def downgrade():
    """Revert bundle_name column to nullable."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column(
            "bundle_name",
            existing_type=sa.String(length=200),
            nullable=True,
        )