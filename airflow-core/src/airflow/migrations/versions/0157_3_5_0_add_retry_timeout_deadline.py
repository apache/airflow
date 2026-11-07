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

"""Add retry timeout deadline to task instances."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import sqlite_rebuilds
from airflow.utils.sqlalchemy import UtcDateTime

revision = "f30d798a2c41"
down_revision = "d8c6f3a129b0"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add the persisted retry timeout deadline."""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.add_column(sa.Column("retry_timeout_deadline", UtcDateTime(), nullable=True))


def downgrade():
    """Remove the persisted retry timeout deadline."""
    with sqlite_rebuilds(op), op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_column("retry_timeout_deadline")
