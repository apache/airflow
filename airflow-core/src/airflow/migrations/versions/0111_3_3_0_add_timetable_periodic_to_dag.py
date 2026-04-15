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
Add timetable_periodic to DagModel.

Revision ID: 9fabad868fdb
Revises: a4c2d171ae18
Create Date: 2026-04-09 10:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "9fabad868fdb"
down_revision = "a4c2d171ae18"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add timetable_periodic column to DagModel."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("timetable_periodic", sa.Boolean, nullable=False, server_default="0"),
        )


def downgrade():
    """Remove timetable_periodic column from DagModel."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("timetable_periodic")
