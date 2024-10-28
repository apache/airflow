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
Add exception_reason and logical_date to BackfillDagRun.

Revision ID: 3a8972ecb8f9
Revises: fb2d4922cd79
Create Date: 2024-10-18 16:24:38.932005

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

revision = "3a8972ecb8f9"
down_revision = "fb2d4922cd79"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Add exception_reason and logical_date to BackfillDagRun."""
    with op.batch_alter_table("backfill_dag_run", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("exception_reason", sa.String(length=250), nullable=True)
        )
        batch_op.add_column(
            sa.Column("logical_date", UtcDateTime(timezone=True), nullable=False)
        )


def downgrade():
    """Unapply Add exception_reason and logical_date to BackfillDagRun."""
    with op.batch_alter_table("backfill_dag_run", schema=None) as batch_op:
        batch_op.drop_column("logical_date")
        batch_op.drop_column("exception_reason")
