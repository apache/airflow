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
Add refresh generation to Dag bundle.

Revision ID: a4f3c8d19e72
Revises: f8c2a1d94e03
Create Date: 2026-09-08 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a4f3c8d19e72"
down_revision = "f8c2a1d94e03"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add the Dag bundle refresh generation."""
    with op.batch_alter_table("dag_bundle", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("refresh_generation", sa.BigInteger(), nullable=False, server_default="0")
        )


def downgrade():
    """Remove the Dag bundle refresh generation."""
    with op.batch_alter_table("dag_bundle", schema=None) as batch_op:
        batch_op.drop_column("refresh_generation")
