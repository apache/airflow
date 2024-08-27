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
Add name field to DatasetModel.

This also renames two indexes. Index names are scoped to the entire database.
Airflow generally includes the table's name to manually scope the index, but
``idx_uri_unique`` (on DatasetModel) and ``idx_name_unique`` (on
DatasetAliasModel) do not do this. They are renamed here so we can create a
unique index on DatasetModel as well.

Revision ID: 0d9e73a75ee4
Revises: 0bfc26bc256e
Create Date: 2024-08-13 09:45:32.213222
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.orm import Session

# revision identifiers, used by Alembic.
revision = "0d9e73a75ee4"
down_revision = "0bfc26bc256e"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"

_NAME_COLUMN_TYPE = sa.String(length=3000).with_variant(
    sa.String(length=3000, collation="latin1_general_cs"),
    dialect_name="mysql",
)


def upgrade():
    # Fix index name on DatasetAlias.
    with op.batch_alter_table("dataset_alias", schema=None) as batch_op:
        batch_op.drop_index("idx_name_unique")
        batch_op.create_index("idx_dataset_alias_name_unique", ["name"], unique=True)
    # Fix index name (of 'uri') on Dataset.
    # Add 'name' column. Set it to nullable for now.
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.drop_index("idx_uri_unique")
        batch_op.create_index("idx_dataset_uri_unique", ["uri"], unique=True)
        batch_op.add_column(sa.Column("name", _NAME_COLUMN_TYPE))
    # Fill name from uri column.
    Session(bind=op.get_bind()).execute(sa.text("update dataset set name=uri"))
    # Set the name column non-nullable.
    # Now with values in there, we can create the unique constraint and index.
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.alter_column("name", existing_type=_NAME_COLUMN_TYPE, nullable=False)
        batch_op.create_index("idx_dataset_name_unique", ["name"], unique=True)


def downgrade():
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.drop_index("idx_dataset_name_unique")
        batch_op.drop_column("name")
        batch_op.drop_index("idx_dataset_uri_unique")
        batch_op.create_index("idx_uri_unique", ["uri"], unique=True)
    with op.batch_alter_table("dataset_alias", schema=None) as batch_op:
        batch_op.drop_index("idx_dataset_alias_name_unique")
        batch_op.create_index("idx_name_unique", ["name"], unique=True)
