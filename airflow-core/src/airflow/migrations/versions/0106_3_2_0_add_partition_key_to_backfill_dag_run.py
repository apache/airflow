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
Add partition_key to backfill_dag_run.

Revision ID: 134de42d3cb0
Revises: e42d9fcd10d9
Create Date: 2026-02-04 11:42:08.773068

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

revision = "134de42d3cb0"
down_revision = "e42d9fcd10d9"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Apply Add partition_key to backfill_dag_run."""
    op.add_column("dag_run", sa.Column("created_at", UtcDateTime(timezone=True), nullable=True))
    op.execute("update dag_run set created_at = run_after;")
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column("created_at", existing_type=UtcDateTime(timezone=True), nullable=False)

    with op.batch_alter_table("backfill_dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("partition_key", StringID(), nullable=True))
        batch_op.alter_column("logical_date", existing_type=sa.TIMESTAMP(), nullable=True)


def downgrade():
    """Unapply Add partition_key to backfill_dag_run."""
    op.execute("DELETE FROM backfill_dag_run WHERE logical_date IS NULL;")
    with op.batch_alter_table("backfill_dag_run", schema=None) as batch_op:
        batch_op.alter_column("logical_date", existing_type=sa.TIMESTAMP(), nullable=False)
        batch_op.drop_column("partition_key")

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("created_at")
