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
Add performance indexes on serialized_dag, dag_code, and task_instance.

Revision ID: 76984aa0347c
Revises: a4c2d171ae18
Create Date: 2026-03-27 00:00:00.000000

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "76984aa0347c"
down_revision = "a4c2d171ae18"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add indexes for query performance on serialized_dag, dag_code, and task_instance."""
    with op.batch_alter_table("serialized_dag", schema=None) as batch_op:
        batch_op.create_index(
            "idx_serialized_dag_dag_id_last_updated", ["dag_id", "last_updated"], unique=False
        )

    with op.batch_alter_table("dag_code", schema=None) as batch_op:
        batch_op.create_index("idx_dag_code_dag_id", ["dag_id"], unique=False)

    # MySQL auto-creates an index on FK columns, so dag_version_id already has one.
    conn = op.get_bind()
    if conn.dialect.name != "mysql":
        with op.batch_alter_table("task_instance", schema=None) as batch_op:
            batch_op.create_index("idx_task_instance_dag_version_id", ["dag_version_id"], unique=False)


def downgrade():
    """Remove performance indexes."""
    conn = op.get_bind()

    # MySQL requires an index on FK columns; since dag_version_id has a FK constraint,
    # we cannot drop the index on MySQL.
    if conn.dialect.name != "mysql":
        with op.batch_alter_table("task_instance", schema=None) as batch_op:
            batch_op.drop_index("idx_task_instance_dag_version_id")

    with op.batch_alter_table("dag_code", schema=None) as batch_op:
        batch_op.drop_index("idx_dag_code_dag_id")

    with op.batch_alter_table("serialized_dag", schema=None) as batch_op:
        batch_op.drop_index("idx_serialized_dag_dag_id_last_updated")
