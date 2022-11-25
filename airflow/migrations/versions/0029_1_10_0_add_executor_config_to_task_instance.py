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
"""Add ``executor_config`` column to ``task_instance`` table

Revision ID: 33ae817a1ff4
Revises: 947454bf1dff
Create Date: 2017-09-11 15:26:47.598494

"""
from __future__ import annotations

import dill
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "27c6a30d7c24"
down_revision = "33ae817a1ff4"
branch_labels = None
depends_on = None
airflow_version = "1.10.0"

TASK_INSTANCE_TABLE = "task_instance"
NEW_COLUMN = "executor_config"


def upgrade():
    op.add_column(TASK_INSTANCE_TABLE, sa.Column(NEW_COLUMN, sa.PickleType(pickler=dill)))


def downgrade():
    op.drop_column(TASK_INSTANCE_TABLE, NEW_COLUMN)
