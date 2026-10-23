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
Rename the stored map index to region_index and add the dynamic region slot key.

Revision ID: 7f8c9a2d410e
Revises: 54a27b6f9d01
Create Date: 2026-09-27 23:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import sqlite_rebuilds

revision = "7f8c9a2d410e"
down_revision = "54a27b6f9d01"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

# ``legacy_task_data_owner``, ``xcom_v1`` and ``rtif_v1`` keep ``map_index``: they record
# the coordinates their rows had before regions existed and are never rewritten.
_TABLES = ("task_instance", "task_state_store")

_SLOT_KEY_CONSTRAINT = "dynamic_region_slot_key_uq"


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
    """Rename the stored coordinate without rewriting rows, keys or indexes, and add the slot key."""
    with sqlite_rebuilds(op):
        _rename("map_index", "region_index")
        op.add_column(
            "dynamic_region",
            sa.Column("slot_key", sa.LargeBinary(32).with_variant(sa.BINARY(32), "mysql", "mariadb")),
        )
        with op.batch_alter_table("dynamic_region") as batch_op:
            batch_op.create_unique_constraint(_SLOT_KEY_CONSTRAINT, ["slot_key"])


def downgrade():
    """Restore the prior column name without changing coordinate identity, and drop the slot key."""
    with sqlite_rebuilds(op):
        with op.batch_alter_table("dynamic_region") as batch_op:
            batch_op.drop_constraint(_SLOT_KEY_CONSTRAINT, type_="unique")
            batch_op.drop_column("slot_key")
        _rename("region_index", "map_index")
