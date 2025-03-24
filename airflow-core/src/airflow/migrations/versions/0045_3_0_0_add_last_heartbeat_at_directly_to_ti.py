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
Add last_heartbeat_at directly to TI.

Revision ID: d8cd3297971e
Revises: 5f57a45b8433
Create Date: 2024-11-01 12:14:59.927266

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = "d8cd3297971e"
down_revision = "5f57a45b8433"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.add_column(sa.Column("last_heartbeat_at", TIMESTAMP(timezone=True), nullable=True))
        batch_op.drop_index("ti_job_id")
        batch_op.create_index("ti_heartbeat", ["last_heartbeat_at"], unique=False)
        batch_op.drop_column("job_id")
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.drop_column("job_id")


def downgrade():
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.add_column(sa.Column("job_id", sa.INTEGER(), autoincrement=False, nullable=True))
        batch_op.drop_index("ti_heartbeat")
        batch_op.create_index("ti_job_id", ["job_id"], unique=False)
        batch_op.drop_column("last_heartbeat_at")
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(sa.Column("job_id", sa.INTEGER(), autoincrement=False, nullable=True))
