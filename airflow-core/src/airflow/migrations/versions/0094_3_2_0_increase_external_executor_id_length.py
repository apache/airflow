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
Increase external_executor_id length to 1000.

Revision ID: f1a2b3c4d5e6
Revises: 665854ef0536
Create Date: 2025-12-26 00:00:00.000000

"""

from __future__ import annotations

from alembic import op

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "f1a2b3c4d5e6"
down_revision = "665854ef0536"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Increase external_executor_id column length from 250 to 1000 chars."""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            type_=StringID(length=1000),
            existing_nullable=True,
        )

    with op.batch_alter_table("task_instance_history") as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            type_=StringID(length=1000),
            existing_nullable=True,
        )


def downgrade():
    """Revert external_executor_id column length from 1000 to 250 chars."""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            type_=StringID(length=250),
            existing_nullable=True,
        )

    with op.batch_alter_table("task_instance_history") as batch_op:
        batch_op.alter_column(
            "external_executor_id",
            type_=StringID(length=250),
            existing_nullable=True,
        )
