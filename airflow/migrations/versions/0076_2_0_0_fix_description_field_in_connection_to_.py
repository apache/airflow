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
Fix description field in ``connection`` to be ``text``.

Revision ID: 64a7d6477aae
Revises: 61ec73d9401f
Create Date: 2020-11-25 08:56:11.866607

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "64a7d6477aae"
down_revision = "61ec73d9401f"
branch_labels = None
depends_on = None
airflow_version = "2.0.0"


def upgrade():
    """Apply Fix description field in ``connection`` to be ``text``."""
    conn = op.get_bind()
    if conn.dialect.name == "sqlite":
        # in sqlite TEXT and STRING column types are the same
        return
    if conn.dialect.name == "mysql":
        op.alter_column(
            "connection",
            "description",
            existing_type=sa.String(length=5000),
            type_=sa.Text(length=5000),
            existing_nullable=True,
        )
    else:
        # postgres does not allow size modifier for text type
        op.alter_column("connection", "description", existing_type=sa.String(length=5000), type_=sa.Text())


def downgrade():
    """Unapply Fix description field in ``connection`` to be ``text``."""
    conn = op.get_bind()
    if conn.dialect.name == "sqlite":
        # in sqlite TEXT and STRING column types are the same
        return
    if conn.dialect.name == "mysql":
        op.alter_column(
            "connection",
            "description",
            existing_type=sa.Text(5000),
            type_=sa.String(length=5000),
            existing_nullable=True,
        )
    else:
        # postgres does not allow size modifier for text type
        op.alter_column(
            "connection",
            "description",
            existing_type=sa.Text(),
            type_=sa.String(length=5000),
            existing_nullable=True,
        )
