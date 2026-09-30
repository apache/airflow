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
Add indexes on asset_partition_dag_run.

Revision ID: f954ddd21484
Revises: 90e4d18ccadf
Create Date: 2026-09-30 19:00:00.000000

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "f954ddd21484"
down_revision = "90e4d18ccadf"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Apply Add indexes on asset_partition_dag_run."""
    dialect_name = op.get_context().dialect.name
    if dialect_name == "mysql":
        # created_dag_run_id is a foreign-key column; InnoDB keeps an implicit index for the constraint and
        # silently drops it once another usable index exists. Recreate the constraint around the new index so
        # the resulting state is deterministic (same approach as migration 0086).
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            batch_op.drop_constraint("apdr_created_dag_run_id_fkey", type_="foreignkey")
    with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
        batch_op.create_index(
            "idx_apdr_target_dag_id_partition_key_id",
            ["target_dag_id", "partition_key", "id"],
            unique=False,
        )
        batch_op.create_index(
            "idx_apdr_created_dag_run_id_created_at_id",
            ["created_dag_run_id", "created_at", "id"],
            unique=False,
        )
    if dialect_name == "mysql":
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            batch_op.create_foreign_key(
                "apdr_created_dag_run_id_fkey", "dag_run", ["created_dag_run_id"], ["id"], ondelete="CASCADE"
            )


def downgrade():
    """Unapply Add indexes on asset_partition_dag_run."""
    dialect_name = op.get_context().dialect.name
    if dialect_name == "mysql":
        # The index now backs the foreign key; dropping it directly fails with ER_DROP_INDEX_FK (1553).
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            batch_op.drop_constraint("apdr_created_dag_run_id_fkey", type_="foreignkey")
            batch_op.drop_index("idx_apdr_created_dag_run_id_created_at_id")
            batch_op.drop_index("idx_apdr_target_dag_id_partition_key_id")
            batch_op.create_foreign_key(
                "apdr_created_dag_run_id_fkey", "dag_run", ["created_dag_run_id"], ["id"], ondelete="CASCADE"
            )
    else:
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            batch_op.drop_index("idx_apdr_created_dag_run_id_created_at_id")
            batch_op.drop_index("idx_apdr_target_dag_id_partition_key_id")
