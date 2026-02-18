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
add timetable_type to dag table for filtering.

Revision ID: e79fc784f145
Revises: 0b112f49112d
Create Date: 2026-01-04 14:36:04.648869

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e79fc784f145"
down_revision = "0b112f49112d"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Apply add timetable_type to dag table for filtering."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("timetable_type", sa.String(length=255)))

    op.execute("UPDATE dag SET timetable_type = '' WHERE timetable_type IS NULL")

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column("timetable_type", existing_type=sa.String(length=255), nullable=False)


def downgrade():
    """Unapply add timetable_type to dag table for filtering."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("timetable_type")
