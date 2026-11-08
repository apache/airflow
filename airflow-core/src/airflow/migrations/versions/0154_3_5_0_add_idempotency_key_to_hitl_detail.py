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
Add idempotency_key to hitl_detail.

Revision ID: a8b48633fa60
Revises: 16d7301f124b
Create Date: 2026-11-04 17:48:09.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "a8b48633fa60"
down_revision = "16d7301f124b"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add the idempotency_key column to hitl_detail."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("hitl_detail", schema=None) as batch_op:
            batch_op.add_column(sa.Column("idempotency_key", sa.Uuid(), nullable=True))


def downgrade():
    """Remove the idempotency_key column from hitl_detail."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("hitl_detail", schema=None) as batch_op:
            batch_op.drop_column("idempotency_key")
