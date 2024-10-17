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
Tweak AssetAliasModel to match AssetModel after AIP-76.

This involves two changes:

1. Add the 'group' column.
2. Reduce the 'name' column to 1500 characters.

The first is straightforward. The second is technically not necessary (the alias
model does not need an extra field for uniqueness), but it is probably better
for the two 'name' fields to have matching behavior, to reduce potential user
confusion.

Hopefully nobody would notice or be affected by this anyway since 1500
characters is plenty enough.

Revision ID: fb2d4922cd79
Revises: 5a5d66100783
Create Date: 2024-10-08 01:46:05.556368
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# Revision identifiers, used by Alembic.
revision = "fb2d4922cd79"
down_revision = "5a5d66100783"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"

_STRING_COLUMN_TYPE = sa.String(length=1500).with_variant(
    sa.String(length=1500, collation="latin1_general_cs"),
    dialect_name="mysql",
)


def upgrade():
    """Tweak AssetAliasModel to match AssetModel."""
    with op.batch_alter_table("dataset_alias", schema=None) as batch_op:
        batch_op.alter_column("name", type_=_STRING_COLUMN_TYPE, nullable=False)
        batch_op.add_column(sa.Column("group", _STRING_COLUMN_TYPE, default=str, nullable=False))


def downgrade():
    """Untweak AssetAliasModel to match AssetModel."""
    with op.batch_alter_table("dataset_alias", schema=None) as batch_op:
        batch_op.drop_column("group")
        batch_op.alter_column(
            "name",
            type_=sa.String(length=3000).with_variant(
                sa.String(length=3000, collation="latin1_general_cs"),
                dialect_name="mysql",
            ),
            nullable=False,
        )
