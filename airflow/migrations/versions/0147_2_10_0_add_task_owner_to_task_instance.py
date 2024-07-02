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

"""Add task_owner to TaskInstance.

Revision ID:
Revises:
Create Date: 2024-06-30 09:40:06.386776

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = ""
down_revision = ""
branch_labels = None
depends_on = None
airflow_version = "2.10.0"

__tablename__ = "task_instance"


def upgrade():
    """Apply Add task_owner to TaskInstance."""
    with op.batch_alter_table(__tablename__) as batch_op:
        batch_op.add_column(sa.Column("task_owner", sa.String(length=50), nullable=True))


def downgrade():
    """Unapply Add rendered_map_index to TaskInstance."""
    with op.batch_alter_table(__tablename__, schema=None) as batch_op:
        batch_op.drop_column("task_owner")
