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

"""add display name for dag and task instance

Revision ID: d83579315023
Revises: 88344c1d9134
Create Date: 2023-09-14 10:31:07.521273

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d83579315023"
down_revision = "88344c1d9134"
branch_labels = None
depends_on = None
airflow_version = "2.9.0"

def upgrade():
    op.add_column("dag", sa.Column("dag_display_name", sa.String(2000), nullable=True))
    op.add_column("task_instance", sa.Column("task_display_name", sa.String(2000), nullable=True))


def downgrade():
    op.drop_column("dag", "dag_display_name")
    op.drop_column("task_instance", "task_display_name")
