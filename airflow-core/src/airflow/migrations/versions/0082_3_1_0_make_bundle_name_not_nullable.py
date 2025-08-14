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
Make bundle_name not nullable.

Revision ID: 7582ea3f3dd5
Revises: a169942745c2
Create Date: 2025-08-13 04:20:04.155103

"""

from __future__ import annotations

from alembic import op
from sqlalchemy.sql import text

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "7582ea3f3dd5"
down_revision = "a169942745c2"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Make bundle_name not nullable."""
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        op.execute(
            text("""
                INSERT INTO dag_bundle (name) VALUES
                    ('example_dags'),
                    ('dags-folder')
                ON CONFLICT (name) DO NOTHING;
                """)
        )
    if dialect_name == "mysql":
        op.execute(
            text("""
                    INSERT IGNORE INTO dag_bundle (name) VALUES
                      ('example_dags'),
                      ('dags-folder');
                    """)
        )
    if dialect_name == "sqlite":
        op.execute(
            text("""
                    INSERT OR IGNORE INTO dag_bundle (name) VALUES
                      ('example_dags'),
                      ('dags-folder');
                    """)
        )

    conn = op.get_bind()
    with op.batch_alter_table("dag", schema=None) as batch_op:
        conn.execute(
            text(
                """
                UPDATE dag
                SET bundle_name =
                    CASE
                        WHEN fileloc LIKE '%/airflow/example_dags/%' THEN 'example_dags'
                        ELSE 'dags-folder'
                    END
                WHERE bundle_name IS NULL
                """
            )
        )
        # drop the foreign key temporarily and recreate it once both columns are changed
        batch_op.drop_constraint(batch_op.f("dag_bundle_name_fkey"), type_="foreignkey")
        batch_op.alter_column("bundle_name", nullable=False, existing_type=StringID())

    with op.batch_alter_table("dag_bundle", schema=None) as batch_op:
        batch_op.alter_column("name", nullable=False, existing_type=StringID())

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.create_foreign_key(
            batch_op.f("dag_bundle_name_fkey"), "dag_bundle", ["bundle_name"], ["name"]
        )


def downgrade():
    """Make bundle_name nullable."""
    import contextlib

    dialect_name = op.get_bind().dialect.name
    exitstack = contextlib.ExitStack()

    if dialect_name == "sqlite":
        # SQLite requires foreign key constraints to be disabled during batch operations
        conn = op.get_bind()
        conn.execute(text("PRAGMA foreign_keys=OFF"))
        exitstack.callback(conn.execute, text("PRAGMA foreign_keys=ON"))

    with exitstack:
        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.drop_constraint(batch_op.f("dag_bundle_name_fkey"), type_="foreignkey")
            batch_op.alter_column("bundle_name", nullable=True, existing_type=StringID())

        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.create_foreign_key(
                batch_op.f("dag_bundle_name_fkey"), "dag_bundle", ["bundle_name"], ["name"]
            )
