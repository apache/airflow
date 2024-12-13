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
remove pickled data from dagrun table.

Revision ID: e39a26ac59f6
Revises: 038dc8bc6284
Create Date: 2024-12-01 08:33:15.425141

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "e39a26ac59f6"
down_revision = "038dc8bc6284"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply remove pickled data from dagrun table."""
    conn = op.get_bind()

    # Update the dag_run.conf column value to NULL
    conn.execute(text("UPDATE dag_run set conf=null WHERE conf IS NOT NULL"))

    conf_type = sa.JSON().with_variant(postgresql.JSONB, "postgresql")

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        # Drop the existing column for SQLite (due to its limitations)
        batch_op.drop_column("conf")
        # Add the new column with the correct type for the all dialect
        batch_op.add_column(sa.Column("conf", conf_type, nullable=True))


def downgrade():
    """Unapply Remove pickled data from dagrun table."""
    conn = op.get_bind()
    conn.execute(text("UPDATE dag_run set conf=null WHERE conf IS NOT NULL"))

    # Update the dag_run.conf column value to NULL to avoid issues during the type change
    conn.execute(text("UPDATE dag_run set conf=null WHERE conf IS NOT NULL"))

    conf_type = sa.LargeBinary().with_variant(postgresql.BYTEA, "postgresql")

    # Apply the same logic for all dialects, including SQLite
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        # Drop the existing column for SQLite (due to its limitations)
        batch_op.drop_column("conf")
        batch_op.add_column(sa.Column("conf", conf_type, nullable=True))
