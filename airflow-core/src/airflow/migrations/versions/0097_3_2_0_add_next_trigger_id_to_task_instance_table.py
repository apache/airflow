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
Add ``next_trigger_id`` column to ``task_instance`` table.

Revision ID: 658517c60c7f
Revises: c47f2e1ab9d4
Create Date: 2025-12-26 12:07:05.849152

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "658517c60c7f"
down_revision = "c47f2e1ab9d4"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add ``next_trigger_id`` column to ``task_instance`` table."""
    op.add_column("task_instance", sa.Column("next_trigger_id", sa.Integer(), nullable=True))


def downgrade():
    """Remove ``next_trigger_id`` column from ``task_instance`` table."""
    op.drop_column("task_instance", "next_trigger_id")
