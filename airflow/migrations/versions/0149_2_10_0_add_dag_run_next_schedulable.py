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
Rename dagrun.last_scheduling_decision.

Revision ID: 840479e61d99
Revises: ec3471c1e067
Create Date: 2024-06-18 08:58:10.356565

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "840479e61d99"
down_revision = "ec3471c1e067"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Apply Rename dagrun.last_scheduling_decision."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("next_schedulable", UtcDateTime()))


def downgrade():
    """Unapply Rename dagrun.last_scheduling_decision."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("next_schedulable")
