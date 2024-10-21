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
add new executor field to db.

Revision ID: 677fdbb7fc54
Revises: 0fd0c178cbe8
Create Date: 2024-04-01 15:26:59.186579

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "677fdbb7fc54"
down_revision = "0fd0c178cbe8"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Apply add executor field to task instance."""
    op.add_column("task_instance", sa.Column("executor", sa.String(length=1000), default=None))


def downgrade():
    """Unapply add executor field to task instance."""
    op.drop_column("task_instance", "executor")
