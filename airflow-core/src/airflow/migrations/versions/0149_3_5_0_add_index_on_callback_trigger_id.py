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
Add index on callback trigger ID.

Revision ID: 16d7301f124b
Revises: c71065a1b14f
Create Date: 2026-10-19 15:11:12.337117

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "16d7301f124b"
down_revision = "c71065a1b14f"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add an index on callback.trigger_id."""
    trigger_id_is_not_null = sa.column("trigger_id").is_not(None)
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("callback", schema=None) as batch_op:
            batch_op.create_index(
                "idx_callback_trigger_id",
                ["trigger_id"],
                unique=False,
                postgresql_where=trigger_id_is_not_null,
                sqlite_where=trigger_id_is_not_null,
            )


def downgrade():
    """Remove the index on callback.trigger_id."""
    with disable_sqlite_fkeys(op):
        conn = op.get_bind()
        with op.batch_alter_table("callback", schema=None) as batch_op:
            if conn.dialect.name == "mysql":
                batch_op.drop_constraint(batch_op.f("callback_trigger_id_fkey"), type_="foreignkey")
                batch_op.drop_index("idx_callback_trigger_id")
                batch_op.create_foreign_key(
                    batch_op.f("callback_trigger_id_fkey"),
                    "trigger",
                    ["trigger_id"],
                    ["id"],
                )
            else:
                batch_op.drop_index("idx_callback_trigger_id")
