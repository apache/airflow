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
Add timetable_type column to DAG table.

Revision ID: 3e4f92b7c1a2
Revises: 7582ea3f3dd5
Create Date: 2025-08-20 12:30:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3e4f92b7c1a2"
down_revision = "7582ea3f3dd5"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("timetable_type", sa.String(length=255), nullable=True))


def downgrade():
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("timetable_type")
