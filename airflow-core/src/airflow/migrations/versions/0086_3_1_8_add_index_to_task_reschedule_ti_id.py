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
Add composite index (ti_id, id DESC) to task_reschedule.

Revision ID: 82dbd68e6171
Revises: cc92b33c6709
Create Date: 2026-01-22 16:25:42.164449

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "82dbd68e6171"
down_revision = "cc92b33c6709"
branch_labels = None
depends_on = None
airflow_version = "3.1.8"


def upgrade():
    """Add composite (ti_id, id DESC) index to task_reschedule."""
    dialect_name = op.get_context().dialect.name
    if dialect_name == "mysql":
        with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
            batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
        op.execute("CREATE INDEX idx_task_reschedule_ti_id_id_desc ON task_reschedule (ti_id, id DESC)")
        with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
            batch_op.create_foreign_key(
                "task_reschedule_ti_fkey", "task_instance", ["ti_id"], ["id"], ondelete="CASCADE"
            )
    elif dialect_name == "sqlite":
        op.execute("CREATE INDEX idx_task_reschedule_ti_id_id_desc ON task_reschedule (ti_id, id DESC)")
    else:
        # PostgreSQL
        with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
            batch_op.create_index(
                "idx_task_reschedule_ti_id_id_desc",
                ["ti_id", "id"],
                unique=False,
                postgresql_ops={"id": "DESC"},
            )


def downgrade():
    """Remove composite index from task_reschedule."""
    dialect_name = op.get_context().dialect.name
    if dialect_name == "mysql":
        with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
            batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
            batch_op.drop_index("idx_task_reschedule_ti_id_id_desc")
            batch_op.create_foreign_key(
                "task_reschedule_ti_fkey", "task_instance", ["ti_id"], ["id"], ondelete="CASCADE"
            )
    else:
        with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
            batch_op.drop_index("idx_task_reschedule_ti_id_id_desc")
