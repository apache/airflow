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
Add disallowed_trigger_types column to dag table.

Revision ID: 8ea135928436
Revises: 8ea135928435
Create Date: 2025-01-24 13:17:13.444341

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

try:
    from sqlalchemy.dialects.mysql import LONGTEXT
except ImportError:
    LONGTEXT = mysql.TEXT

# Revision identifiers, used by Alembic.
revision = "8ea135928436"
down_revision = "8ea135928435"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Add disallowed_trigger_types column to dag table."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    # Use JSON type for PostgreSQL, MySQL with JSON support, and others with proper JSON types.
    # Fall back to TEXT for other database engines like SQLite that don't support JSON.
    if dialect_name == "postgresql":
        json_type = sa.JSON
    elif dialect_name == "mysql":
        json_type = sa.JSON
    else:
        json_type = sa.Text

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("disallowed_trigger_types", json_type, nullable=True))


def downgrade():
    """Unapply Add disallowed_trigger_types column to dag table."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("disallowed_trigger_types") 