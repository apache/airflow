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
Add ``timetable_description`` column to DagModel for UI.

Revision ID: 786e3737b18f
Revises: 5e3ec427fdd3
Create Date: 2021-10-15 13:33:04.754052

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "786e3737b18f"
down_revision = "5e3ec427fdd3"
branch_labels = None
depends_on = None
airflow_version = "2.3.0"


def upgrade():
    """Apply Add ``timetable_description`` column to DagModel for UI."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("timetable_description", sa.String(length=1000), nullable=True))


def downgrade():
    """Unapply Add ``timetable_description`` column to DagModel for UI."""
    is_sqlite = bool(op.get_bind().dialect.name == "sqlite")
    if is_sqlite:
        op.execute("PRAGMA foreign_keys=off")
    with op.batch_alter_table("dag") as batch_op:
        batch_op.drop_column("timetable_description")
    if is_sqlite:
        op.execute("PRAGMA foreign_keys=on")
