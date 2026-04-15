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
Add language field to task instance.

Revision ID: 7d3c6395b7f6
Revises: 9fabad868fdb
Create Date: 2026-04-15 05:57:22.353951

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7d3c6395b7f6"
down_revision = "9fabad868fdb"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Apply add language field to task instance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.add_column(sa.Column("language", sa.String(length=64), nullable=True))


def downgrade():
    """Unapply add language field to task instance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_column("language")
