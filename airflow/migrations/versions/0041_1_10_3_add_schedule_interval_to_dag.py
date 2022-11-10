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
"""Add schedule interval to dag

Revision ID: dd4ecb8fbee3
Revises: c8ffec048a3b
Create Date: 2018-12-27 18:39:25.748032

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "dd4ecb8fbee3"
down_revision = "c8ffec048a3b"
branch_labels = None
depends_on = None
airflow_version = "1.10.3"


def upgrade():
    op.add_column("dag", sa.Column("schedule_interval", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("dag", "schedule_interval")
