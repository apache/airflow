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
Add partition fields to DagModel.

Revision ID: 6222ce48e289
Revises: 134de42d3cb0
Create Date: 2026-02-25 06:29.58.176890

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

revision = "6222ce48e289"
down_revision = "134de42d3cb0"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add partition fields to DagModel."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("timetable_partitioned", sa.Boolean, nullable=False, server_default="0"),
        )
        batch_op.add_column(sa.Column("next_dagrun_partition_key", sa.String(255)))
        batch_op.add_column(sa.Column("next_dagrun_partition_date", UtcDateTime))
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("partition_date", UtcDateTime))


def downgrade():
    """Remove partition fields from DagModel."""
    from airflow.migrations.utils import disable_sqlite_fkeys

    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.drop_column("timetable_partitioned")
            batch_op.drop_column("next_dagrun_partition_key")
            batch_op.drop_column("next_dagrun_partition_date")
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.drop_column("partition_date")

    # When downgrading from 3.2.0 to 3.1.x, the run_id generation semantics change:
    # 3.2.0 generates run_id from run_after (data interval end),
    # 3.1.x generates run_id from next_dagrun (logical date / data interval start).
    # If next_dagrun/next_dagrun_create_after are left pointing at values set by 3.2.0,
    # the 3.1.x scheduler will try to create runs with run_ids that already exist in
    # dag_run, causing DB insert violation. Nulling these fields forces the 3.1.x
    # scheduler to recalculate them from the last completed run using 3.1.x semantics.
    op.execute(
        "UPDATE dag SET next_dagrun = NULL, next_dagrun_create_after = NULL, "
        "next_dagrun_data_interval_start = NULL, next_dagrun_data_interval_end = NULL"
    )
