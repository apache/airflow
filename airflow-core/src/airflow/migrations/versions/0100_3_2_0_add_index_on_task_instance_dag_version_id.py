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
Add index on task_instance.dag_version_id.

Revision ID: 9a7aaf94f36a
Revises: 55297ae24532
Create Date: 2026-01-21
"""

from __future__ import annotations

from alembic import op

revision = "9a7aaf94f36a"
down_revision = "55297ae24532"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add index on task_instance.dag_version_id for query performance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.create_index("ti_dag_version_id", ["dag_version_id"], unique=False)


def downgrade():
    """Remove index on task_instance.dag_version_id."""
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        # MySQL requires an index on FK columns; since dag_version_id has a FK constraint,
        # we cannot drop the index. MySQL will keep using it for the FK.
        return
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_index("ti_dag_version_id")
