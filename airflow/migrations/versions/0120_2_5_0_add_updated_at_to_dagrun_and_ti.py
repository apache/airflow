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

"""Add updated_at column to DagRun and TaskInstance

Revision ID: ee8d93fcc81e
Revises: e07f49787c9d
Create Date: 2022-09-08 19:08:37.623121

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = "ee8d93fcc81e"
down_revision = "e07f49787c9d"
branch_labels = None
depends_on = None
airflow_version = "2.5.0"


def upgrade():
    """Apply add updated_at column to DagRun and TaskInstance"""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.add_column(sa.Column("updated_at", TIMESTAMP, default=sa.func.now))

    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.add_column(sa.Column("updated_at", TIMESTAMP, default=sa.func.now))


def downgrade():
    """Unapply add updated_at column to DagRun and TaskInstance"""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_column("updated_at")

    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.drop_column("updated_at")
