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
Add dataset_alias.

Revision ID: 05e19f3176be
Revises: d482b7261ff9
Create Date: 2024-07-05 08:17:12.017789

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "05e19f3176be"
down_revision = "d482b7261ff9"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Add dataset_alias table."""
    op.create_table(
        "dataset_alias",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "name",
            sa.String(length=3000).with_variant(
                sa.String(length=3000, collation="latin1_general_cs"), "mysql"
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("dataset_alias_pkey")),
    )
    op.create_index("idx_name_unique", "dataset_alias", ["name"], unique=True)


def downgrade():
    """Drop dataset_alias table."""
    op.drop_table("dataset_alias")
