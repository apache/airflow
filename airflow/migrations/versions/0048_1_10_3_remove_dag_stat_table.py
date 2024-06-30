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
Remove ``dag_stat`` table.

Revision ID: a56c9515abdc
Revises: c8ffec048a3b
Create Date: 2018-12-27 10:27:59.715872

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a56c9515abdc"
down_revision = "c8ffec048a3b"
branch_labels = None
depends_on = None
airflow_version = "1.10.3"


def upgrade():
    """Drop dag_stats table."""
    op.drop_table("dag_stats")


def downgrade():
    """Create dag_stats table."""
    op.create_table(
        "dag_stats",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("state", sa.String(length=50), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False, default=0),
        sa.Column("dirty", sa.Boolean(), nullable=False, default=False),
        sa.PrimaryKeyConstraint("dag_id", "state"),
    )
