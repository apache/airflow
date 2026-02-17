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
Add index to task_reschedule ti_id .

Revision ID: 82dbd68e6171
Revises: 55297ae24532
Create Date: 2026-01-22 16:25:42.164449

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "82dbd68e6171"
down_revision = "55297ae24532"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add index to task_reschedule ti_id."""
    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.create_index("idx_task_reschedule_ti_id", ["ti_id"], unique=False)


def downgrade():
    """Remove index from task_reschedule ti_id."""
    dialect_name = op.get_context().dialect.name
    if dialect_name == "mysql":
        with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
            batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
            batch_op.drop_index("idx_task_reschedule_ti_id")
            batch_op.create_foreign_key(
                "task_reschedule_ti_fkey", "task_instance", ["ti_id"], ["id"], ondelete="CASCADE"
            )
    else:
        with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
            batch_op.drop_index("idx_task_reschedule_ti_id")
