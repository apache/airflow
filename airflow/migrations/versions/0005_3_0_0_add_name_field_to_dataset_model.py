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

Revision ID: 0d9e73a75ee4
Revises: 044f740568ec
Create Date: 2024-08-13 09:45:32.213222

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0d9e73a75ee4"
down_revision = "0bfc26bc256e"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Add name field to dataset model."""
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "name",
                sa.String(length=3000).with_variant(
                    sa.String(length=3000, collation="latin1_general_cs"), "mysql"
                ),
                nullable=False,
            )
        )


def downgrade():
    """Remove name field to dataset model."""
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.drop_column("name")
