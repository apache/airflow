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
Add bundle_name to callback table.

Revision ID: 1d6611b6ab7c
Revises: 888b59e02a5b
Create Date: 2026-03-17 00:23:45.305588

"""

from __future__ import annotations

import json

import sqlalchemy as sa
from alembic import context, op

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import ExtendedJSON

# revision identifiers, used by Alembic.
revision = "1d6611b6ab7c"
down_revision = "888b59e02a5b"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add bundle_name to callback rows and backfill dag-processor callbacks."""
    with op.batch_alter_table("callback", schema=None) as batch_op:
        batch_op.add_column(sa.Column("bundle_name", StringID(), nullable=True))

    if context.is_offline_mode():
        print(
            """
            --  WARNING: Unable to backfill callback.bundle_name values while in offline mode!
            --  The bundle_name column will be added without migrating existing dag-processor callback rows.
            --  Run this migration in online mode if you need existing pending callbacks backfilled.
            """
        )
        return

    conn = op.get_bind()
    callback = sa.table(
        "callback",
        sa.column("id", sa.Uuid()),
        sa.column("type", sa.String(length=20)),
        sa.column("data", ExtendedJSON()),
        sa.column("bundle_name", StringID()),
    )

    rows = conn.execute(
        sa.select(callback.c.id, callback.c.data).where(callback.c.type == "dag_processor")
    ).mappings()
    for row in rows:
        data = row["data"] or {}
        if isinstance(data, str):
            data = json.loads(data)

        req_data = data.get("req_data")
        if isinstance(req_data, str):
            req_data = json.loads(req_data)
        elif not isinstance(req_data, dict):
            continue

        bundle_name = req_data.get("bundle_name")
        if bundle_name is None:
            continue

        conn.execute(callback.update().where(callback.c.id == row["id"]).values(bundle_name=bundle_name))


def downgrade():
    """Remove bundle_name from callback rows."""
    with op.batch_alter_table("callback", schema=None) as batch_op:
        batch_op.drop_column("bundle_name")
