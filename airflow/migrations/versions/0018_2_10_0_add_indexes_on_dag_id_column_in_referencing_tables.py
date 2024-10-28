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
Add indexes on dag_id column in referencing tables.

Revision ID: 0fd0c178cbe8
Revises: 686269002441
Create Date: 2024-05-15 16:52:39.077349

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0fd0c178cbe8"
down_revision = "686269002441"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Apply Add indexes on dag_id column in referencing tables."""
    with op.batch_alter_table("dag_schedule_dataset_reference") as batch_op:
        batch_op.create_index(
            "idx_dag_schedule_dataset_reference_dag_id", ["dag_id"], unique=False
        )

    with op.batch_alter_table("dag_tag") as batch_op:
        batch_op.create_index("idx_dag_tag_dag_id", ["dag_id"], unique=False)

    with op.batch_alter_table("dag_warning") as batch_op:
        batch_op.create_index("idx_dag_warning_dag_id", ["dag_id"], unique=False)

    with op.batch_alter_table("dataset_dag_run_queue") as batch_op:
        batch_op.create_index(
            "idx_dataset_dag_run_queue_target_dag_id", ["target_dag_id"], unique=False
        )

    with op.batch_alter_table("task_outlet_dataset_reference") as batch_op:
        batch_op.create_index(
            "idx_task_outlet_dataset_reference_dag_id", ["dag_id"], unique=False
        )


def _handle_foreign_key_constraint_index_deletion(
    batch_op, constraint_name, index_name, local_fk_column_name
):
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        batch_op.drop_constraint(constraint_name, type_="foreignkey")
        batch_op.drop_index(index_name)
        batch_op.create_foreign_key(
            constraint_name, "dag", [local_fk_column_name], ["dag_id"], ondelete="CASCADE"
        )
    else:
        batch_op.drop_index(index_name)


def downgrade():
    """Unapply Add indexes on dag_id column in referencing tables."""
    with op.batch_alter_table("dag_schedule_dataset_reference") as batch_op:
        _handle_foreign_key_constraint_index_deletion(
            batch_op,
            "dsdr_dag_id_fkey",
            "idx_dag_schedule_dataset_reference_dag_id",
            "dag_id",
        )

    with op.batch_alter_table("dag_tag") as batch_op:
        _handle_foreign_key_constraint_index_deletion(
            batch_op, "dag_tag_dag_id_fkey", "idx_dag_tag_dag_id", "dag_id"
        )

    with op.batch_alter_table("dag_warning") as batch_op:
        _handle_foreign_key_constraint_index_deletion(
            batch_op, "dcw_dag_id_fkey", "idx_dag_warning_dag_id", "dag_id"
        )

    with op.batch_alter_table("dataset_dag_run_queue") as batch_op:
        _handle_foreign_key_constraint_index_deletion(
            batch_op,
            "ddrq_dag_fkey",
            "idx_dataset_dag_run_queue_target_dag_id",
            "target_dag_id",
        )

    with op.batch_alter_table("task_outlet_dataset_reference") as batch_op:
        _handle_foreign_key_constraint_index_deletion(
            batch_op,
            "todr_dag_id_fkey",
            "idx_task_outlet_dataset_reference_dag_id",
            "dag_id",
        )
