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
Add triggering user to dag_run.

Revision ID: 66a7743fe20e
Revises: 583e80dfcef4
Create Date: 2025-06-18 19:43:07.975512

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "66a7743fe20e"
down_revision = "583e80dfcef4"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Add triggering user to dag_run."""
    with op.batch_alter_table("backfill", schema=None) as batch_op:
        batch_op.add_column(sa.Column("triggering_user_name", sa.String(length=512), nullable=True))

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("triggering_user_name", sa.String(length=512), nullable=True))


def downgrade():
    """Unapply triggering user to dag_run."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("triggering_user_name")

    with op.batch_alter_table("backfill", schema=None) as batch_op:
        batch_op.drop_column("triggering_user_name")
