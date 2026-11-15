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
Add commit_allows_update to connection_test_request.

Revision ID: 9cffe90fa0c0
Revises: a5d7b9c13e40
Create Date: 2026-10-16 10:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "9cffe90fa0c0"
down_revision = "a5d7b9c13e40"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add the commit_allows_update column to connection_test_request."""
    with op.batch_alter_table("connection_test_request", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("commit_allows_update", sa.Boolean(), nullable=False, server_default="0")
        )


def downgrade():
    """Remove the commit_allows_update column from connection_test_request."""
    with op.batch_alter_table("connection_test_request", schema=None) as batch_op:
        batch_op.drop_column("commit_allows_update")
