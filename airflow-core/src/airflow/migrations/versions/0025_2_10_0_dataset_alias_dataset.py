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
Add dataset_alias_dataset association table.

Revision ID: 8684e37832e6
Revises: 41b3bc7c0272
Create Date: 2024-07-18 06:21:06.242569

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8684e37832e6"
down_revision = "41b3bc7c0272"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Add dataset_alias_dataset association table."""
    op.create_table(
        "dataset_alias_dataset",
        sa.Column("alias_id", sa.Integer(), nullable=False),
        sa.Column("dataset_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["alias_id"],
            ["dataset_alias.id"],
            name=op.f("dataset_alias_dataset_alias_id_fkey"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            ["dataset.id"],
            name=op.f("dataset_alias_dataset_dataset_id_fkey"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("alias_id", "dataset_id", name=op.f("dataset_alias_dataset_pkey")),
    )
    op.create_index(
        "idx_dataset_alias_dataset_alias_dataset_id", "dataset_alias_dataset", ["dataset_id"], unique=False
    )
    op.create_index("idx_dataset_alias_dataset_alias_id", "dataset_alias_dataset", ["alias_id"], unique=False)


def downgrade():
    """Drop dataset_alias_dataset association table."""
    op.drop_table("dataset_alias_dataset")
