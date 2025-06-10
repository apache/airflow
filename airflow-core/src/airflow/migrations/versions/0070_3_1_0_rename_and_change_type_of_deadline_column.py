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
Rename Deadline column in the Deadline table from deadline to deadline_time and change its type from DateTime to UTC DateTime.

Revision ID: 0242ac120002
Revises: dfee8bd5d574
Create Date: 2024-12-18 19:10:26.962464
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP

revision = "0242ac120002"
down_revision = "dfee8bd5d574"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply change to deadline column in the deadline table."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_index("deadline_idx")
        batch_op.alter_column(
            "deadline",
            existing_type=sa.DateTime(),
            type_=TIMESTAMP(timezone=True),
            existing_nullable=False,
            new_column_name="deadline_time",
        )
    op.create_index("deadline_time_idx", "deadline", ["deadline_time"], unique=False)


def downgrade():
    """Unapply change to deadline column in the deadline table."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_index("deadline_time_idx")
        batch_op.alter_column(
            "deadline_time",
            existing_type=TIMESTAMP(timezone=True),
            type_=sa.DateTime(),
            existing_nullable=False,
            new_column_name="deadline",
        )
    op.create_index("deadline_idx", "deadline", ["deadline"], unique=False)
