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
Add ``fail_fast`` column to dag table.

Revision ID: 69ddce9a7247
Revises: 5cc8117e9285
Create Date: 2025-10-16 03:22:59.016272
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "69ddce9a7247"
down_revision = "5cc8117e9285"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add fail_fast column to dag table."""
    op.add_column("dag", sa.Column("fail_fast", sa.Boolean(), nullable=False, server_default="0"))


def downgrade():
    """Drop fail_fast column in dag table."""
    op.drop_column("dag", "fail_fast")
