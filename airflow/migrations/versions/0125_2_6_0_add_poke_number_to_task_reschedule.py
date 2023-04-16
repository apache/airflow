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

"""Add poke_number column to task_reschedule table

Revision ID: 2ffc1d0aaa75
Revises: 98ae134e6fff
Create Date: 2023-04-16 19:16:19.465500

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2ffc1d0aaa75"
down_revision = "98ae134e6fff"
branch_labels = None
depends_on = None
airflow_version = "2.6.0"


def upgrade():
    """Apply Add poke_number column to task_reschedule table"""
    op.add_column("task_reschedule", sa.Column("poke_number", sa.Integer))


def downgrade():
    """Unapply Add poke_number column to task_reschedule table"""
    op.drop_column("task_reschedule", "poke_number")
