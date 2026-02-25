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
Add ``queue`` column to ``trigger`` table.

Revision ID: c47f2e1ab9d4
Revises: edc4f85a4619
Create Date: 2025-12-09 20:30:40.500001

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c47f2e1ab9d4"
down_revision = "edc4f85a4619"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add ``queue`` column in trigger table."""
    with op.batch_alter_table("trigger") as batch_op:
        batch_op.add_column(sa.Column("queue", sa.String(length=256), nullable=True))


def downgrade():
    """Remove ``queue`` column from trigger table."""
    with op.batch_alter_table("trigger") as batch_op:
        batch_op.drop_column("queue")
