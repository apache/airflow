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
Restructure callback table.

Revision ID: b87d2135fa50
Revises: 69ddce9a7247
Create Date: 2025-09-10 13:58:23.435028

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils import UUIDType

from airflow.utils.sqlalchemy import ExtendedJSON

# revision identifiers, used by Alembic.
revision = "b87d2135fa50"
down_revision = "69ddce9a7247"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """
    Restructure callback table.

    Add/drop/modify columns, create foreign key for trigger and rename the table name.
    """
    # Drop all rows. this will only affect users who have Dag Processor callbacks in non-terminal states
    # during migration.
    op.execute("DELETE FROM callback_request")
    # TODO: migrate existing rows for pending DagProcessor callbacks

    op.rename_table("callback_request", "callback")
    with op.batch_alter_table("callback", schema=None) as batch_op:
        batch_op.add_column(sa.Column("type", sa.String(length=20), nullable=False))
        batch_op.add_column(sa.Column("fetch_method", sa.String(length=20), nullable=False))
        batch_op.add_column(sa.Column("data", ExtendedJSON(), nullable=False))
        batch_op.add_column(sa.Column("state", sa.String(length=10), nullable=True))
        batch_op.add_column(sa.Column("output", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("trigger_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(batch_op.f("callback_trigger_id_fkey"), "trigger", ["trigger_id"], ["id"])

        # Replace INTEGER id with UUID id
        batch_op.drop_column("id")
        batch_op.add_column(sa.Column("id", UUIDType(binary=False), nullable=False))
        batch_op.create_primary_key("callback_pkey", ["id"])

        batch_op.drop_column("callback_data")
        batch_op.drop_column("callback_type")


def downgrade():
    """
    Unapply the callback table restructure.

    Add/drop/modify columns, create foreign key for trigger and rename the table name.
    """
    op.execute("DELETE FROM callback")  # Drop all rows. See comment in upgrade().

    with op.batch_alter_table("callback", schema=None) as batch_op:
        batch_op.add_column(sa.Column("callback_type", sa.String(length=20), nullable=False))
        batch_op.add_column(sa.Column("callback_data", ExtendedJSON(), nullable=False))

        # Replace UUID id with INTEGER id
        batch_op.drop_column("id")
        batch_op.add_column(sa.Column("id", sa.INTEGER(), nullable=False, autoincrement=True))
        batch_op.create_primary_key("callback_request_pkey", ["id"])

        batch_op.drop_constraint(batch_op.f("callback_trigger_id_fkey"), type_="foreignkey")
        batch_op.drop_column("trigger_id")
        batch_op.drop_column("output")
        batch_op.drop_column("state")
        batch_op.drop_column("data")
        batch_op.drop_column("fetch_method")
        batch_op.drop_column("type")

    op.rename_table("callback", "callback_request")
