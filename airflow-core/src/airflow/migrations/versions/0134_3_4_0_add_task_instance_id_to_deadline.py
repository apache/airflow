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
Add task instance association to deadline.

Revision ID: b7a04c9e12d5
Revises: f8c2a1d94e03
Create Date: 2026-09-05 12:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b7a04c9e12d5"
down_revision = "f8c2a1d94e03"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Apply add task instance association to deadline."""
    with op.batch_alter_table("deadline") as batch_op:
        batch_op.add_column(sa.Column("task_instance_id", sa.Uuid(), nullable=True))
        batch_op.create_foreign_key(
            "deadline_task_instance_id_fkey",
            "task_instance",
            ["task_instance_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_index("idx_deadline_task_instance_id", ["task_instance_id"])


def downgrade():
    """Unapply add task instance association to deadline."""
    with op.batch_alter_table("deadline") as batch_op:
        batch_op.drop_constraint("deadline_task_instance_id_fkey", type_="foreignkey")
        batch_op.drop_index("idx_deadline_task_instance_id")
        batch_op.drop_column("task_instance_id")
