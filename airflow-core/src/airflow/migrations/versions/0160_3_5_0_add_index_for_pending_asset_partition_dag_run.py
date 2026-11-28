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
Add index for pending asset partition Dag run lookups.

Revision ID: c7e7e43fb9ba
Revises: b8f1c0a4d276
Create Date: 2026-11-16 07:48:17.082327
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "c7e7e43fb9ba"
down_revision = "b8f1c0a4d276"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add index on asset_partition_dag_run (target_dag_id, created_dag_run_id, created_at, id)."""
    with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
        batch_op.create_index(
            "idx_apdr_target_dag_id_created_dag_run_id_created_at",
            ["target_dag_id", "created_dag_run_id", "created_at", "id"],
            unique=False,
        )


def downgrade():
    """Remove index on asset_partition_dag_run (target_dag_id, created_dag_run_id, created_at, id)."""
    with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
        batch_op.drop_index("idx_apdr_target_dag_id_created_dag_run_id_created_at")
