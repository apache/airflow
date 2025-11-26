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
add last_parse_duration to dag model.

Revision ID: eaf332f43c7c
Revises: a3c7f2b18d4e
Create Date: 2025-08-20 15:53:26.138686

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "eaf332f43c7c"
down_revision = "a3c7f2b18d4e"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply add last_parse_duration to dag model."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("last_parse_duration", sa.Float(), nullable=True))


def downgrade():
    """Unapply add last_parse_duration to dag model."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    if dialect_name == "sqlite":
        # SQLite requires foreign key constraints to be disabled during batch operations
        conn.execute(text("PRAGMA foreign_keys=OFF"))
        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.drop_column("last_parse_duration")
        conn.execute(text("PRAGMA foreign_keys=ON"))
    else:
        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.drop_column("last_parse_duration")
