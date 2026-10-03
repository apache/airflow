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
Preserve DagRun.conf JSON number representation on PostgreSQL.

Revision ID: a2c4e6f8091b
Revises: 90e4d18ccadf
Create Date: 2026-10-03 10:00:00.000000

"""

from __future__ import annotations

from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a2c4e6f8091b"
down_revision = "90e4d18ccadf"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Use textual PostgreSQL JSON instead of canonicalizing JSONB."""
    if op.get_bind().dialect.name == "postgresql":
        op.alter_column(
            "dag_run",
            "conf",
            existing_type=postgresql.JSONB(),
            type_=postgresql.JSON(),
            postgresql_using="conf::json",
        )


def downgrade():
    """Restore PostgreSQL JSONB storage."""
    if op.get_bind().dialect.name == "postgresql":
        op.alter_column(
            "dag_run",
            "conf",
            existing_type=postgresql.JSON(),
            type_=postgresql.JSONB(),
            postgresql_using="conf::jsonb",
        )
