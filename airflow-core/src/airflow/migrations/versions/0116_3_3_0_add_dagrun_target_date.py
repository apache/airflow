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
Add target_date column to dag_run table.

Revision ID: e3f9a1b2c4d8
Revises: a1b2c3d4e5f6
Create Date: 2026-05-13 10:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "e3f9a1b2c4d8"
down_revision = "acc215baed80"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add target_date column to dag_run."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("target_date", sa.Date(), nullable=True))


def downgrade():
    """Remove target_date column from dag_run."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("target_date")
