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
Rename trigger owner_ref to used_by_ref.

Revision ID: c64b10719069
Revises: d8c6f3a129b0
Create Date: 2026-11-10 12:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "c64b10719069"
down_revision = "d8c6f3a129b0"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def _rename(old: str, new: str) -> None:
    # A plain rename on every backend (RENAME COLUMN on SQLite), so the table is not rebuilt and rows are kept.
    op.alter_column("trigger", old, new_column_name=new, existing_type=sa.JSON(), existing_nullable=True)


def upgrade():
    """Rename trigger owner_ref to used_by_ref."""
    _rename("owner_ref", "used_by_ref")


def downgrade():
    """Rename trigger used_by_ref back to owner_ref."""
    _rename("used_by_ref", "owner_ref")
