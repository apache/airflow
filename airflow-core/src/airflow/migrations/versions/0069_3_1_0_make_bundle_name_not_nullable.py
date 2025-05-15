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
Make dag.bundle_name not nullable.

Revision ID: 319a48850861
Revises: 29ce7909c52b
Create Date: 2025-05-06 03:57:45.999583

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import text

# revision identifiers, used by Alembic.
revision = "319a48850861"
down_revision = "29ce7909c52b"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    dialect_name = op.get_context().dialect.name
    if dialect_name == "postgresql":
        op.execute("""
                    INSERT INTO dag_bundle (name) VALUES
                      ('example_dags'),
                      ('dags-folder')
                    ON CONFLICT (name) DO NOTHING;
                    """)
    if dialect_name == "mysql":
        op.execute("""
                    INSERT IGNORE INTO dag_bundle (name) VALUES
                      ('example_dags'),
                      ('dags-folder');
                    """)
    if dialect_name == "sqlite":
        op.execute("""
                    INSERT OR IGNORE INTO dag_bundle (name) VALUES
                      ('example_dags'),
                      ('dags-folder');
                    """)

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
        batch_op.alter_column("bundle_name", nullable=False, existing_type=sa.String(length=250))


def downgrade():
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column("bundle_name", nullable=True, existing_type=sa.String(length=250))
