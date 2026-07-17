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
Add dataset_alias_dataset_event.

Revision ID: ec3471c1e067
Revises: 05e19f3176be
Create Date: 2024-07-11 09:42:00.643179

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ec3471c1e067"
down_revision = "05e19f3176be"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Add dataset_alias_dataset_event table."""
    op.create_table(
        "dataset_alias_dataset_event",
        sa.Column("alias_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["alias_id"],
            ["dataset_alias.id"],
            name=op.f("dataset_alias_dataset_event_alias_id_fkey"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["event_id"],
            ["dataset_event.id"],
            name=op.f("dataset_alias_dataset_event_event_id_fkey"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("alias_id", "event_id", name=op.f("dataset_alias_dataset_event_pkey")),
    )
    with op.batch_alter_table("dataset_alias_dataset_event", schema=None) as batch_op:
        batch_op.create_index("idx_dataset_alias_dataset_event_alias_id", ["alias_id"], unique=False)
        batch_op.create_index("idx_dataset_alias_dataset_event_event_id", ["event_id"], unique=False)


def downgrade():
    """Drop dataset_alias_dataset_event table."""
    op.drop_table("dataset_alias_dataset_event")
