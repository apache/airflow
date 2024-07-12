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
Add priority_weight to ti_trigger_id index.

Revision ID: 4a21d8b12614
Revises: d482b7261ff9
Create Date: 2024-07-10 13:23:57.346261

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "4a21d8b12614"
down_revision = "d482b7261ff9"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Apply recreate ti_trigger_id index as composite index with priority_weight."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_constraint("task_instance_trigger_id_fkey", type_="foreignkey")
        batch_op.drop_index("ti_trigger_id")
        batch_op.create_foreign_key(
            "task_instance_trigger_id_fkey", "trigger", ["trigger_id"], ["id"], ondelete="CASCADE"
        )
        batch_op.create_index("ti_trigger_id", ["trigger_id", "priority_weight"])


def downgrade():
    """Unapply creation of composite index for ti_trigger_id index."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_constraint("task_instance_trigger_id_fkey", type_="foreignkey")
        batch_op.drop_index("ti_trigger_id")
        batch_op.create_foreign_key(
            "task_instance_trigger_id_fkey", "trigger", ["trigger_id"], ["id"], ondelete="CASCADE"
        )
        batch_op.create_index("ti_trigger_id", ["trigger_id"])
