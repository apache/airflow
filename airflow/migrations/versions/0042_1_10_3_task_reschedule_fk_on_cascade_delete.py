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
"""task reschedule foreign key on cascade delete

Revision ID: 939bb1e647c8
Revises: dd4ecb8fbee3
Create Date: 2019-02-04 20:21:50.669751

"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "939bb1e647c8"
down_revision = "dd4ecb8fbee3"
branch_labels = None
depends_on = None
airflow_version = "1.10.3"


def upgrade():
    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.drop_constraint("task_reschedule_dag_task_date_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "task_reschedule_dag_task_date_fkey",
            "task_instance",
            ["task_id", "dag_id", "execution_date"],
            ["task_id", "dag_id", "execution_date"],
            ondelete="CASCADE",
        )


def downgrade():
    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.drop_constraint("task_reschedule_dag_task_date_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "task_reschedule_dag_task_date_fkey",
            "task_instance",
            ["task_id", "dag_id", "execution_date"],
            ["task_id", "dag_id", "execution_date"],
        )
