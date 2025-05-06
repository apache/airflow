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
Rename DagModel schedule_interval to timetable_summary.

Revision ID: 0bfc26bc256e
Revises: d0f1c55954fa
Create Date: 2024-08-15 06:24:14.363316

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0bfc26bc256e"
down_revision = "d0f1c55954fa"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Rename DagModel schedule_interval to timetable_summary."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column(
            "schedule_interval",
            new_column_name="timetable_summary",
            type_=sa.Text,
            nullable=True,
        )


def downgrade():
    """Rename timetable_summary back to schedule_interval."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column(
            "timetable_summary",
            new_column_name="schedule_interval",
            type_=sa.Text,
            nullable=True,
        )
    op.execute("UPDATE dag SET schedule_interval=NULL;")
