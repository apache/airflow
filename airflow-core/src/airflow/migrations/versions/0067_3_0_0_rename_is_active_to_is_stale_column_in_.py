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
Rename ``is_active`` to ``is_stale`` column in ``dag`` table.

Revision ID: 959e216a3abb
Revises: 0e9519b56710
Create Date: 2025-04-09 17:11:08.379065
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "959e216a3abb"
down_revision = "0e9519b56710"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Rename is_active to is_stale column in DAG table."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column("is_active", new_column_name="is_stale", type_=sa.Boolean)

    op.execute("UPDATE dag SET is_stale = NOT is_stale")


def downgrade():
    """Revert renaming of is_active to is_stale column in DAG table."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column("is_stale", new_column_name="is_active", type_=sa.Boolean)

    op.execute("UPDATE dag SET is_active = NOT is_active")
