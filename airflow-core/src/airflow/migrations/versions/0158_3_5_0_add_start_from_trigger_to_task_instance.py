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
Add start_from_trigger to task_instance.

Revision ID: 93ebd666b0d8
Revises: c64b10719069
Create Date: 2026-11-20 10:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import sqlite_rebuilds

# revision identifiers, used by Alembic.
revision = "93ebd666b0d8"
down_revision = "c64b10719069"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add start_from_trigger to task_instance."""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.add_column(sa.Column("start_from_trigger", sa.Boolean(), nullable=True))


def downgrade():
    """Remove start_from_trigger from task_instance."""
    # Dropping a column rebuilds the table on SQLite, unlike adding one, so the foreign keys of
    # everything that points at task_instance have to be held off while the rows are copied.
    with sqlite_rebuilds(op), op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_column("start_from_trigger")
