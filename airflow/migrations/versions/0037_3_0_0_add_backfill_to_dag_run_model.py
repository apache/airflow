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
Add backfill to dag run model.

Revision ID: c3389cd7793f
Revises: 0d9e73a75ee4
Create Date: 2024-09-21 07:52:29.869725

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c3389cd7793f"
down_revision = "0d9e73a75ee4"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Add backfill to dag run model."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("backfill_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            batch_op.f("dag_run_backfill_id_fkey"), "backfill", ["backfill_id"], ["id"]
        )


def downgrade():
    """Unapply Add backfill to dag run model."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("dag_run_backfill_id_fkey"), type_="foreignkey")
        batch_op.drop_column("backfill_id")
