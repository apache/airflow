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
Add dag_result to XComModel.

Revision ID: a4c2d171ae18
Revises: 1d6611b6ab7c
Create Date: 2026-03-17 00:23:45.305588

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a4c2d171ae18"
down_revision = "1d6611b6ab7c"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add dag_result to XComModel."""
    with op.batch_alter_table("xcom", schema=None) as batch_op:
        batch_op.add_column(sa.Column("dag_result", sa.Boolean, nullable=True))


def downgrade():
    """Remove dag_result from XComModel."""
    with op.batch_alter_table("xcom", schema=None) as batch_op:
        batch_op.drop_column("dag_result")
