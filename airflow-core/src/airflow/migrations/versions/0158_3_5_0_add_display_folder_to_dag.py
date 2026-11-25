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
Add display_folder to dag.

Revision ID: e7a3d91c4f28
Revises: c64b10719069
Create Date: 2026-11-24 12:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "e7a3d91c4f28"
down_revision = "c64b10719069"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add display_folder column to dag table."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("display_folder", sa.String(length=2000), nullable=True))


def downgrade():
    """Remove display_folder column from dag table."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("display_folder")
