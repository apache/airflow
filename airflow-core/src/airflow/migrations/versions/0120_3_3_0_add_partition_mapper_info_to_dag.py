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
Add partition_mapper_info to DagModel.

The new JSON column caches per-asset partition mapper metadata produced
during Dag serialization (one entry per asset, see ``PartitionMapperInfo``)
so UI endpoints can resolve mapper attributes such as ``is_rollup`` without
deserializing the timetable on every request. Defaults to ``[]`` for
timetables without per-asset partition mappers.

Revision ID: c20871fbf23a
Revises: c9d4e5f6a7b8
Create Date: 2026-05-06 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "c20871fbf23a"
down_revision = "c9d4e5f6a7b8"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """
    Add partition_mapper_info JSON column to dag table.

    The column is added nullable, existing rows are backfilled with ``[]``,
    then the column is altered to NOT NULL. MySQL refuses literal defaults on
    JSON columns, and the model intentionally carries no ``server_default``,
    so we use the same backfill strategy on every backend to avoid leaving a
    stray DB-side default that diverges from the ORM definition. The final
    ``alter_column`` triggers a table rebuild on SQLite; foreign keys are
    disabled around the whole upgrade so dependent tables' references stay
    intact regardless of which step ends up rebuilding the table.
    """
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.add_column(sa.Column("partition_mapper_info", sa.JSON(), nullable=True))
        op.execute(sa.text("UPDATE dag SET partition_mapper_info = '[]' WHERE partition_mapper_info IS NULL"))
        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.alter_column("partition_mapper_info", existing_type=sa.JSON(), nullable=False)


def downgrade():
    """Remove partition_mapper_info column from dag table."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.drop_column("partition_mapper_info")
