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

"""Add indexes on dag_id column in referencing tables.

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
airflow_version = "2.9.2"


def upgrade():
    """Apply Add indexes on dag_id column in referencing tables."""
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("dag_owner_attributes", schema=None) as batch_op:
        batch_op.create_index("idx_dag_owner_attributes_dag_id", ["dag_id"], unique=False)

    with op.batch_alter_table("dag_schedule_dataset_reference", schema=None) as batch_op:
        batch_op.create_index("idx_dag_schedule_dataset_reference_dag_id", ["dag_id"], unique=False)

    with op.batch_alter_table("dag_tag", schema=None) as batch_op:
        batch_op.create_index("idx_dag_tag_dag_id", ["dag_id"], unique=False)

    with op.batch_alter_table("dag_warning", schema=None) as batch_op:
        batch_op.create_index("idx_dag_warning_dag_id", ["dag_id"], unique=False)

    with op.batch_alter_table("dataset_dag_run_queue", schema=None) as batch_op:
        batch_op.create_index("idx_dataset_dag_run_queue_target_dag_id", ["target_dag_id"], unique=False)

    with op.batch_alter_table("task_outlet_dataset_reference", schema=None) as batch_op:
        batch_op.create_index("idx_task_outlet_dataset_reference_dag_id", ["dag_id"], unique=False)

    # ### end Alembic commands ###


def downgrade():
    """Unapply Add indexes on dag_id column in referencing tables."""
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("task_outlet_dataset_reference", schema=None) as batch_op:
        batch_op.drop_index("idx_task_outlet_dataset_reference_dag_id")

    with op.batch_alter_table("dataset_dag_run_queue", schema=None) as batch_op:
        batch_op.drop_index("idx_dataset_dag_run_queue_target_dag_id")

    with op.batch_alter_table("dag_warning", schema=None) as batch_op:
        batch_op.drop_index("idx_dag_warning_dag_id")

    with op.batch_alter_table("dag_tag", schema=None) as batch_op:
        batch_op.drop_index("idx_dag_tag_dag_id")

    with op.batch_alter_table("dag_schedule_dataset_reference", schema=None) as batch_op:
        batch_op.drop_index("idx_dag_schedule_dataset_reference_dag_id")

    with op.batch_alter_table("dag_owner_attributes", schema=None) as batch_op:
        batch_op.drop_index("idx_dag_owner_attributes_dag_id")

    # ### end Alembic commands ###
