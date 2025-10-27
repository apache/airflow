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
Add required fields to enable UI integrations for the Deadline Alerts feature.

Revision ID: 55297ae24532
Revises: b87d2135fa50
Create Date: 2025-10-17 16:04:55.016272
"""

from __future__ import annotations

from collections import defaultdict

import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils import UUIDType

from airflow.migrations.db_types import TIMESTAMP
from airflow.sdk.definitions.deadline import DeadlineAlert

revision = "55297ae24532"
down_revision = "b87d2135fa50"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Make changes to enable adding DeadlineAlerts to the UI."""
    # TODO:   We may finally have come up with a better naming convention. For ease of migration,
    #   we are going to keep deadline_alert here to match the model's name, but in the near future
    #   when this migration work is done we should deprecate the name DeadlineAlert (and all related
    #   classes, tables, etc) and replace it with DeadlineDefinition.  Then we will have the
    #   user-provided DeadlineDefinition, and the actual instance of a Definition is (still) the Deadline.
    #   This feels more intuitive than DeadlineAlert defining the Deadline.

    op.create_table(
        "deadline_alert",
        sa.Column("id", UUIDType(binary=False)),
        sa.Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("name", sa.String(250), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("reference", sa.JSON(), nullable=True),
        sa.Column("interval", sa.Integer(), nullable=False),
        sa.Column("callback", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("deadline_alert_pkey")),
    )

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("deadline_alert_id", UUIDType(binary=False), nullable=True))
        batch_op.add_column(
            sa.Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now())
        )
        batch_op.add_column(
            sa.Column(
                "last_updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()
            )
        )

    op.create_foreign_key(
        op.f("deadline_deadline_alert_id_fkey"),
        "deadline",
        "deadline_alert",
        ["deadline_alert_id"],
        ["id"],
        ondelete="SET NULL",
    )

    migrate_existing_deadline_alert_data_from_serialized_dag()


def migrate_existing_deadline_alert_data_from_serialized_dag():
    """Extract DeadlineAlert data from serialized DAG data and populate deadline_alert table."""

    # TODO:  Implement a batch-based migration to account for memory constraints
    #  during massive migrations using the Airflow batch size config value.

    """
    General process:

    for each existing serialized_dag
        extract any DeadlineAlerts from the data as a list
        for each DeadlineAlert found:
            deserialize the DeadlineAlert
            generate a new uuid
            add and populate a row in the new table with these values
            update the existing deadline table's new fkey to include the new UUID
    """


def downgrade():
    """Remove changes that were added to enable adding DeadlineAlerts to the UI."""
    op.drop_constraint(op.f("deadline_deadline_alert_id_fkey"), "deadline", type_="foreignkey")

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_column("deadline_alert_id", if_exists=True)
        batch_op.drop_column("last_updated_at", if_exists=True)
        batch_op.drop_column("created_at", if_exists=True)

    op.drop_table("deadline_alert")
