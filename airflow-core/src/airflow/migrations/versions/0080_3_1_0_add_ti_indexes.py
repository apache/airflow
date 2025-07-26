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
Add indexes on span_status and dag_version_id in task_instance.

Revision ID: dd00f83f516a
Revises: f56f68b9e02f
Create Date: 2025-07-25 06:39:47.040231

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "dd00f83f516a"
down_revision = "f56f68b9e02f"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply Add indexes on span_status and dag_version_id in task_instance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.create_index("idx_span_status", ["id", "span_status"])
        batch_op.create_index("idx_dag_version_id", ["dag_version_id"])


def downgrade():
    """Unapply Add indexes on span_status and dag_version_id in task_instance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_index("idx_span_status")
        batch_op.drop_index("idx_dag_version_id")
