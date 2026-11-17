#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Add finished_at to trigger.

Revision ID: 3f9a1c2b7d4e
Revises: d8c6f3a129b0
Create Date: 2026-11-14 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys
from airflow.utils.sqlalchemy import UtcDateTime

revision = "3f9a1c2b7d4e"
down_revision = "d8c6f3a129b0"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add nullable finished_at to trigger; it marks a failed, unreferenced trigger."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("trigger", schema=None) as batch_op:
            batch_op.add_column(sa.Column("finished_at", UtcDateTime(), nullable=True))


def downgrade():
    """Remove finished_at from trigger."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("trigger", schema=None) as batch_op:
            batch_op.drop_column("finished_at")
