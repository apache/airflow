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
update trigger_timeout column in task_instance table to UTC.

Revision ID: 038dc8bc6284
Revises: e229247a6cb1
Create Date: 2024-11-30 10:47:17.542690

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = "038dc8bc6284"
down_revision = "e229247a6cb1"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply update task instance trigger timeout to utcdatetime."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column(
            "trigger_timeout",
            existing_type=sa.DateTime(),
            type_=TIMESTAMP(timezone=True),
            existing_nullable=True,
        )


def downgrade():
    """Unapply update task instance trigger timeout to utcdatetime."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column(
            "trigger_timeout",
            existing_type=TIMESTAMP(timezone=True),
            type_=sa.DateTime(),
            existing_nullable=True,
        )
