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
Add name and group fields to DatasetModel.

The unique index on DatasetModel is also modified to include name. Existing rows
have their name copied from URI.

While not strictly related to other changes, the index name on DatasetAliasModel
is also renamed. Index names are scoped to the entire database. Airflow generally
includes the table's name to manually scope the index, but ``idx_uri_unique``
(on DatasetModel) and ``idx_name_unique`` (on DatasetAliasModel) do not do this.
The one on DatasetModel is already renamed in this PR (to include name), so we
also rename the one on DatasetAliasModel here for consistency.

Revision ID: 0d9e73a75ee4
Revises: 44eabb1904b4
Create Date: 2024-08-13 09:45:32.213222
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0d9e73a75ee4"
down_revision = "44eabb1904b4"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"

_STRING_COLUMN_TYPE = sa.String(length=1500).with_variant(
    sa.String(length=1500, collation="latin1_general_cs"),
    dialect_name="mysql",
)


def upgrade():
    dialect = op.get_bind().dialect.name
    # Fix index name on DatasetAlias.
    with op.batch_alter_table("dataset_alias", schema=None) as batch_op:
        batch_op.drop_index("idx_name_unique")
        batch_op.create_index("idx_dataset_alias_name_unique", ["name"], unique=True)
    # Add 'name' and 'group' columns. Set them to nullable for now.
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.add_column(sa.Column("name", _STRING_COLUMN_TYPE))
        batch_op.add_column(sa.Column("group", _STRING_COLUMN_TYPE))
    # Fill name from uri column, and group to 'asset'.
    if dialect == "mysql":
        stmt = "UPDATE dataset SET name = uri, `group` = 'asset'"
    else:
        stmt = "UPDATE dataset SET name = uri, \"group\" = 'asset'"
    op.execute(stmt)
    # Set the name column non-nullable.
    # Now with values in there, we can create the new unique constraint and index.
    # Due to MySQL restrictions, we are also reducing the length on uri.
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.alter_column("name", existing_type=_STRING_COLUMN_TYPE, nullable=False)
        batch_op.alter_column("uri", type_=_STRING_COLUMN_TYPE, nullable=False)
        batch_op.alter_column("group", type_=_STRING_COLUMN_TYPE, default="asset", nullable=False)
        batch_op.drop_index("idx_uri_unique")
        batch_op.create_index("idx_dataset_name_uri_unique", ["name", "uri"], unique=True)


def downgrade():
    # Remove orphaned datasets if there is an active one that shares the same URI.
    # We will to create a unique constraint on URI and can't fail with a duplicate.
    # This drops history, which is unfortunate, but reasonable for downgrade.
    if op.get_bind().dialect.name == "mysql":
        # Crazy query for MySQL because...
        # - It does not allow referencing d1 in an exists subquery -> Use WHERE id IN instead.
        # - Join to find if each row has a non-orphaned row with the same URI. If there is, select its id.
        # - Modifying the table currently used for selection is not allowed; the additional layer
        #   SELECT id FROM (...) AS d3 makes MySQL think the selecting table is different.
        op.execute(
            "delete from dataset where id in ("
            "    select id from ("
            "        select d1.id from dataset d1"
            "        inner join (select id as active_id, uri from dataset where is_orphaned = false) d2"
            "        on d1.uri = d2.uri"
            "        where d1.id != active_id"
            "    ) as d3"
            ")"
        )
    else:
        op.execute(
            "delete from dataset as d1 where d1.is_orphaned = true "
            "and exists (select 1 from dataset as d2 where d1.uri = d2.uri and d2.is_orphaned = false)"
        )

    # Keep only the datasets with min id if multiple orphaned datasets with the same uri exist.
    # This usually happens when all the dags are turned off.
    op.execute(
        """
        with unique_dataset as (select min(id) as min_id, uri as uri from dataset group by id),
        duplicate_dataset_id as (
            select id from dataset join unique_dataset
            on dataset.uri = unique_dataset.uri
            where dataset.id > unique_dataset.min_id
        )
        delete from dataset where id in (select * from duplicate_dataset_id)
        """
    )
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.drop_index("idx_dataset_name_uri_unique")
        batch_op.create_index("idx_uri_unique", ["uri"], unique=True)
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.drop_column("group")
        batch_op.drop_column("name")
        batch_op.alter_column(
            "uri",
            type_=sa.String(length=3000).with_variant(
                sa.String(length=3000, collation="latin1_general_cs"),
                dialect_name="mysql",
            ),
            nullable=False,
        )
    with op.batch_alter_table("dataset_alias", schema=None) as batch_op:
        batch_op.drop_index("idx_dataset_alias_name_unique")
        batch_op.create_index("idx_name_unique", ["name"], unique=True)
