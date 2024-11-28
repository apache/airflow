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
make dagrun conf as json.

Revision ID: c9475b782ad4
Revises: eed27faa34e3
Create Date: 2024-11-26 06:34:43.882306

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects.mysql import LONGBLOB

# revision identifiers, used by Alembic.
revision = "c9475b782ad4"
down_revision = "eed27faa34e3"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply make dagrun conf as json."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    condition_templates = {
        "postgresql": "get_byte(conf, 0) = 128",
        "mysql": "HEX(SUBSTRING(conf, 1, 1)) = '80'",
        "sqlite": "substr(conf, 1, 1) = char(128)",
    }

    condition = condition_templates.get(dialect)
    if not condition:
        raise RuntimeError(f"Unsupported dialect: {dialect}")

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
        op.execute("UPDATE dag_run SET conf_json = CAST(conf AS CHAR CHARACTER SET utf8mb4)")
        op.drop_column("dag_run", "conf")
        op.alter_column("dag_run", "conf_json", existing_type=sa.JSON(), new_column_name="conf")
    elif dialect == "sqlite":
        # Rename the existing `conf` column to `conf_old`
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.alter_column("conf", new_column_name="conf_old")

        # Add the new `conf` column with JSON type
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.add_column(sa.Column("conf", sa.JSON(), nullable=True))

        # Migrate data from `conf_old` to `conf`
        conn.execute(
            text(
                """
                UPDATE dag_run
                SET conf = json(CAST(conf_old AS TEXT))
                WHERE conf_old IS NOT NULL
                """
            )
        )

        # Drop the old `conf_old` column
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.drop_column("conf_old")


def downgrade():
    """Unapply make dagrun conf as json."""
    conn = op.get_bind()
    dialect = conn.dialect.name

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
        op.execute("UPDATE dag_run SET conf_blob = CAST(conf AS BINARY);")
        op.drop_column("dag_run", "conf")
        op.alter_column("dag_run", "conf_blob", existing_type=LONGBLOB, new_column_name="conf")

    elif dialect == "sqlite":
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.alter_column("conf", new_column_name="conf_old")

        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.add_column(sa.Column("conf", sa.LargeBinary, nullable=True))

        conn.execute(
            text(
                """
                UPDATE dag_run
                SET conf = CAST(conf_old AS BLOB)
                WHERE conf_old IS NOT NULL
                """
            )
        )

        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.drop_column("conf_old")
