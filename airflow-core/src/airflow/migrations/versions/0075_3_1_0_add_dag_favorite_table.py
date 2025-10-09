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
Add dag_favorite table.

Revision ID: ffdb0566c7c0
Revises: 66a7743fe20e
Create Date: 2025-06-05 15:06:08.903908

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.models.base import COLLATION_ARGS

revision = "ffdb0566c7c0"
down_revision = "66a7743fe20e"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply add dag_favorite table."""
    op.create_table(
        "dag_favorite",
        sa.Column("user_id", sa.String(length=250), nullable=False),
        sa.Column("dag_id", sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.ForeignKeyConstraint(
            ["dag_id"], ["dag.dag_id"], name=op.f("dag_favorite_dag_id_fkey"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("user_id", "dag_id", name=op.f("dag_favorite_pkey")),
    )


def downgrade():
    """Unapply add dag_favorite table."""
    op.drop_table("dag_favorite")
