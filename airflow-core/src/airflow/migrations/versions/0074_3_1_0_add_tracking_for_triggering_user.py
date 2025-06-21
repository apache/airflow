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
add tracking for triggering user.

Revision ID: d84c6f94b411
Revises: 583e80dfcef4
Create Date: 2025-06-21 21:27:19.537660

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d84c6f94b411"
down_revision = "583e80dfcef4"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply add tracking for triggering user."""
    with op.batch_alter_table("backfill", schema=None) as batch_op:
        batch_op.add_column(sa.Column("triggered_by", sa.String(length=512), nullable=True))

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "triggered_with",
                sa.Enum(
                    "CLI",
                    "OPERATOR",
                    "REST_API",
                    "UI",
                    "TEST",
                    "TIMETABLE",
                    "ASSET",
                    "BACKFILL",
                    name="dagruntriggeredwithtype",
                    native_enum=False,
                    length=50,
                ),
                nullable=True,
            )
        )
        batch_op.alter_column(
            "triggered_by",
            existing_type=sa.VARCHAR(length=50),
            type_=sa.String(length=512),
            existing_nullable=True,
        )

    op.execute("UPDATE dag_run SET triggered_with = triggered_by")
    op.execute("UPDATE dag_run SET triggered_by = NULL")


def downgrade():
    """Unapply add tracking for triggering user."""
    op.execute("UPDATE dag_run SET triggered_by = triggered_with")

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column(
            "triggered_by",
            existing_type=sa.String(length=512),
            type_=sa.VARCHAR(length=50),
            existing_nullable=True,
        )
        batch_op.drop_column("triggered_with")

    with op.batch_alter_table("backfill", schema=None) as batch_op:
        batch_op.drop_column("triggered_by")
