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
Add AssetActive to track orphaning instead of a flag.

Revision ID: 5a5d66100783
Revises: c3389cd7793f
Create Date: 2024-10-01 08:39:48.997198

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5a5d66100783"
down_revision = "c3389cd7793f"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"

_STRING_COLUMN_TYPE = sa.String(length=1500).with_variant(
    sa.String(length=1500, collation="latin1_general_cs"),
    "mysql",
)


def upgrade():
    op.create_table(
        "asset_active",
        sa.Column("name", _STRING_COLUMN_TYPE, nullable=False),
        sa.Column("uri", _STRING_COLUMN_TYPE, nullable=False),
        sa.PrimaryKeyConstraint("name", "uri", name="asset_active_pkey"),
        sa.ForeignKeyConstraint(
            columns=["name", "uri"],
            refcolumns=["dataset.name", "dataset.uri"],
            name="asset_active_asset_name_uri_fkey",
            ondelete="CASCADE",
        ),
        sa.Index("idx_asset_active_name_unique", "name", unique=True),
        sa.Index("idx_asset_active_uri_unique", "uri", unique=True),
    )
    op.execute("insert into asset_active (name, uri) select name, uri from dataset where is_orphaned = false")
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.drop_column("is_orphaned")


def downgrade():
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("is_orphaned", sa.Boolean, default=False, nullable=False, server_default="0")
        )
    op.execute(
        "update dataset set is_orphaned = true "
        "where not exists (select 1 from asset_active "
        "where dataset.name = asset_active.name and dataset.uri = asset_active.uri)"
    )

    op.drop_table("asset_active")
