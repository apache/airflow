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
Rename stored map indexes to region indexes.

Revision ID: 7f8c9a2d410e
Revises: 54a27b6f9d01
Create Date: 2026-09-27 23:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "7f8c9a2d410e"
down_revision = "54a27b6f9d01"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

# ``legacy_task_data_owner``, ``xcom_v1`` and ``rtif_v1`` keep ``map_index``: they record
# the coordinates their rows had before regions existed and are never rewritten.
_TABLES = ("task_instance", "task_state_store")


def _rename(old: str, new: str) -> None:
    for table in _TABLES:
        op.alter_column(
            table,
            old,
            new_column_name=new,
            existing_type=sa.Integer(),
            existing_nullable=False,
            existing_server_default="-1",
        )


def upgrade():
    """Rename the stored coordinate without rewriting rows, keys or indexes."""
    _rename("map_index", "region_index")


def downgrade():
    """Restore the prior column name without changing coordinate identity."""
    _rename("region_index", "map_index")
