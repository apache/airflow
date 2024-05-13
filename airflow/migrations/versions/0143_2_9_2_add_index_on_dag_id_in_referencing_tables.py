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

"""Add index on dag_id in referencing tables.

Revision ID: 250a7e04585c
Revises: 686269002441
Create Date: 2024-05-09 19:18:54.028277

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "250a7e04585c"
down_revision = "686269002441"
branch_labels = None
depends_on = None
airflow_version = "2.9.2"


def upgrade():
    """Apply Add index on dag_id in referencing tables."""
    conn = op.get_bind()
    # MySQL already indexes the foreign keys, hence only create indexes for postgres and sqlite.
    if conn.dialect.name in ("postgresql", "sqlite"):
        op.create_index("idx_dag_tag_dag_id", "dag_tag", ["dag_id"])
        op.create_index("idx_dag_owner_attributes_dag_id", "dag_owner_attributes", ["dag_id"])
        op.create_index("idx_dag_warning_dag_id", "dag_warning", ["dag_id"])
        op.create_index(
            "idx_dag_schedule_dataset_reference_dag_id", "dag_schedule_dataset_reference", ["dag_id"]
        )
        op.create_index(
            "idx_task_outlet_dataset_reference_dag_id", "task_outlet_dataset_reference", ["dag_id"]
        )
        op.create_index("idx_dataset_dag_run_queue_target_dag_id", "dataset_dag_run_queue", ["target_dag_id"])


def downgrade():
    """Unapply Add index on dag_id in referencing tables."""
    conn = op.get_bind()
    # MySQL already indexes the foreign keys, hence only drop indexes for postgres and sqlite that were
    # created in upgrade.
    if conn.dialect.name in ("postgresql", "sqlite"):
        op.drop_index("idx_dag_tag_dag_id", table_name="dag_tag")
        op.drop_index("idx_dag_owner_attributes_dag_id", table_name="dag_owner_attributes")
        op.drop_index("idx_dag_warning_dag_id", table_name="dag_warning")
        op.drop_index(
            "idx_dag_schedule_dataset_reference_dag_id", table_name="dag_schedule_dataset_reference"
        )
        op.drop_index("idx_task_outlet_dataset_reference_dag_id", table_name="task_outlet_dataset_reference")
        op.drop_index("idx_dataset_dag_run_queue_target_dag_id", table_name="dataset_dag_run_queue")
