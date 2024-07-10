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
Add description field to ``connection`` table.

Revision ID: 61ec73d9401f
Revises: 2c6edca13270
Create Date: 2020-09-10 14:56:30.279248

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "61ec73d9401f"
down_revision = "2c6edca13270"
branch_labels = None
depends_on = None
airflow_version = "2.0.0"


def upgrade():
    """Apply Add description field to ``connection`` table."""
    conn = op.get_bind()

    with op.batch_alter_table("connection") as batch_op:
        if conn.dialect.name == "mysql":
            # Handles case where on mysql with utf8mb4 this would exceed the size of row
            # We have to set text type in this migration even if originally it was string
            # This is permanently fixed in the follow-up migration 64a7d6477aae
            batch_op.add_column(sa.Column("description", sa.Text(length=5000), nullable=True))
        else:
            batch_op.add_column(sa.Column("description", sa.String(length=5000), nullable=True))


def downgrade():
    """Unapply Add description field to ``connection`` table."""
    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.drop_column("description")
