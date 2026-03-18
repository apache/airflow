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
Make external_executor_id TEXT to allow for longer external_executor_ids.

Revision ID: a5a3e5eb9b8d
Revises: 55297ae24532
Create Date: 2026-01-28 16:35:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a5a3e5eb9b8d"
down_revision = "55297ae24532"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Change external_executor_id column from VARCHAR(250) to TEXT."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            existing_type=sa.VARCHAR(length=250),
            type_=sa.Text(),
            existing_nullable=True,
        )

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            existing_type=sa.VARCHAR(length=250),
            type_=sa.Text(),
            existing_nullable=True,
        )


def downgrade():
    """Revert external_executor_id column from TEXT to VARCHAR(250)."""
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            existing_type=sa.Text(),
            type_=sa.VARCHAR(length=250),
            existing_nullable=True,
        )

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            existing_type=sa.Text(),
            type_=sa.VARCHAR(length=250),
            existing_nullable=True,
        )
