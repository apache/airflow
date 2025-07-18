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
Update dag_run_note.user_id and task_instance_note.user_id columns to String.

Revision ID: 44eabb1904b4
Revises: 16cbcb1c8c36
Create Date: 2024-09-27 09:57:29.830521

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "44eabb1904b4"
down_revision = "16cbcb1c8c36"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    with op.batch_alter_table("dag_run_note") as batch_op:
        batch_op.alter_column("user_id", type_=sa.String(length=128))
    with op.batch_alter_table("task_instance_note") as batch_op:
        batch_op.alter_column("user_id", type_=sa.String(length=128))


def downgrade():
    with op.batch_alter_table("dag_run_note") as batch_op:
        batch_op.alter_column("user_id", type_=sa.Integer(), postgresql_using="user_id::integer")
    with op.batch_alter_table("task_instance_note") as batch_op:
        batch_op.alter_column("user_id", type_=sa.Integer(), postgresql_using="user_id::integer")
