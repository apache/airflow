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
remove pickled data from dagrun table.

Revision ID: e39a26ac59f6
Revises: eed27faa34e3
Create Date: 2024-12-01 08:33:15.425141

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects.mysql import LONGBLOB

# revision identifiers, used by Alembic.
revision = "e39a26ac59f6"
down_revision = "eed27faa34e3"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply remove pickled data from dagrun table."""
    # Summary of the change:
    # 1. Updating dag_run.conf column value to NULL.
    # 2. Update the dag_run.conf column type to JSON from bytea

    conn = op.get_bind()
    dialect = conn.dialect.name

    # Update the dag_run.conf column value to NULL
    conn.execute(text("UPDATE dag_run set conf=null WHERE conf IS NOT NULL"))

    # Update the conf column type to JSON
    if dialect == "postgresql":
        op.execute(
            """
            ALTER TABLE dag_run
            ALTER COLUMN conf TYPE JSONB
            USING CASE
                WHEN conf IS NOT NULL THEN CAST(CONVERT_FROM(conf, 'UTF8') AS JSONB)
                ELSE NULL
            END
            """
        )
    elif dialect == "mysql":
        op.add_column("dag_run", sa.Column("conf_json", sa.JSON(), nullable=True))
        op.drop_column("dag_run", "conf")
        op.alter_column("dag_run", "conf_json", existing_type=sa.JSON(), new_column_name="conf")
    elif dialect == "sqlite":
        # Rename the existing `conf` column to `conf_old`
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.alter_column("conf", new_column_name="conf_old")

        # Add the new `conf` column with JSON type
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.add_column(sa.Column("conf", sa.JSON(), nullable=True))

        # Drop the old `conf_old` column
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.drop_column("conf_old")


def downgrade():
    """Unapply Remove pickled data from dagrun table."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    conn.execute(text("UPDATE dag_run set conf=null WHERE conf IS NOT NULL"))

    # Revert the conf column back to LargeBinary
    if dialect == "postgresql":
        op.execute(
            """
            ALTER TABLE dag_run
            ALTER COLUMN conf TYPE BYTEA
            USING CASE
                WHEN conf IS NOT NULL THEN CONVERT_TO(conf::TEXT, 'UTF8')
                ELSE NULL
            END
            """
        )

    elif dialect == "mysql":
        op.add_column("dag_run", sa.Column("conf_blob", LONGBLOB, nullable=True))
        op.drop_column("dag_run", "conf")
        op.alter_column("dag_run", "conf_blob", existing_type=LONGBLOB, new_column_name="conf")

    elif dialect == "sqlite":
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.alter_column("conf", new_column_name="conf_old")

        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.add_column(sa.Column("conf", sa.LargeBinary, nullable=True))

        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.drop_column("conf_old")
