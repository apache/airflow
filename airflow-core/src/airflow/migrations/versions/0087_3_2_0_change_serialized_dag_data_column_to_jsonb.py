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
Change ``serialized_dag`` data column to JSONB for PostgreSQL.

Revision ID: ab6dc0c82d0e
Revises: 15d84ca19038
Create Date: 2025-09-23 12:00:00.000000

"""

from __future__ import annotations

from textwrap import dedent

from alembic import context, op

# revision identifiers, used by Alembic.
revision = "ab6dc0c82d0e"
down_revision = "15d84ca19038"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Apply Change serialized_dag data column to JSONB for PostgreSQL."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    if dialect == "postgresql":
        if context.is_offline_mode():
            print(
                dedent("""
                ------------
                --  WARNING: Converting JSON to JSONB in offline mode!
                --  This migration converts the 'data' column from JSON to JSONB type.
                --  Verify the generated SQL is correct for your use case.
                ------------
                """)
            )

        # Convert the data column from JSON to JSONB
        # This is safe because the column already contains JSON data
        op.execute(
            """
            ALTER TABLE serialized_dag
            ALTER COLUMN data TYPE JSONB
            USING data::JSONB
            """
        )


def downgrade():
    """Unapply Change serialized_dag data column to JSONB for PostgreSQL."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    if dialect == "postgresql":
        if context.is_offline_mode():
            print(
                dedent("""
                ------------
                --  WARNING: Converting JSONB to JSON in offline mode!
                --  This migration converts the 'data' column from JSONB to JSON type.
                --  Verify the generated SQL is correct for your use case.
                ------------
                """)
            )

        # Convert the data column from JSONB back to JSON
        op.execute(
            """
            ALTER TABLE serialized_dag
            ALTER COLUMN data TYPE JSON
            USING data::JSON
            """
        )
