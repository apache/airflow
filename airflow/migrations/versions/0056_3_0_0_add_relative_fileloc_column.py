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
Add relative fileloc column.

Revision ID: 8ea135928435
Revises: e39a26ac59f6
Create Date: 2025-01-24 13:17:13.444341

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "8ea135928435"
down_revision = "e39a26ac59f6"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Add relative fileloc column."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("relative_fileloc", sa.String(length=2000), nullable=True))


def downgrade():
    """Unapply Add relative fileloc column."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("relative_fileloc")
