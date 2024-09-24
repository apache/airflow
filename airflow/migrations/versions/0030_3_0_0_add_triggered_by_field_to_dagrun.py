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
Add triggered_by field to DagRun.

Revision ID: a2c32e6c7729
Revises: 0bfc26bc256e
Create Date: 2024-08-28 14:51:00.424173

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a2c32e6c7729"
down_revision = "0bfc26bc256e"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Add triggered_by field to DagRun."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "triggered_by",
                sa.Enum(
                    "CLI",
                    "OPERATOR",
                    "REST_API",
                    "UI",
                    "TEST",
                    "TIMETABLE",
                    "DATASET",
                    name="dagruntriggeredbytype",
                    native_enum=False,
                    length=50,
                ),
                nullable=True,
            )
        )


def downgrade():
    """Drop triggered_by field to DagRun."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("triggered_by")
