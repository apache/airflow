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
add new task_instance field scheduled_dttm.

Revision ID: 33b04e4bfa19
Revises: 8ea135928435
Create Date: 2025-01-22 11:22:01.272681

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "33b04e4bfa19"
down_revision = "8ea135928435"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply add new task_instance field scheduled_dttm."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.add_column(sa.Column("scheduled_dttm", UtcDateTime(timezone=True), nullable=True))

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(sa.Column("scheduled_dttm", UtcDateTime(timezone=True), nullable=True))


def downgrade():
    """Unapply add new task_instance field scheduled_dttm."""
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.drop_column("scheduled_dttm")

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_column("scheduled_dttm")
