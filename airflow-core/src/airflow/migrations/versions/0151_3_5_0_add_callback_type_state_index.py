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
Add an index on the type and state of callbacks.

Revision ID: f9520b8eed39
Revises: 5fbfa42a2d01
Create Date: 2026-10-23 09:00:00.000000

"""

from __future__ import annotations

from alembic import op

revision = "f9520b8eed39"
down_revision = "5fbfa42a2d01"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Index the columns each scheduler loop looks callbacks up by."""
    op.create_index("idx_callback_type_state", "callback", ["type", "state"])


def downgrade():
    """Drop the index on the type and state of callbacks."""
    op.drop_index("idx_callback_type_state", table_name="callback")
