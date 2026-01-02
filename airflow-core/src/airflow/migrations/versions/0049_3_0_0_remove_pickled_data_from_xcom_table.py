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
Remove pickled data from xcom table.

Revision ID: eed27faa34e3
Revises: 9fc3fc5de720
Create Date: 2024-11-18 18:41:50.849514

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy import text
from sqlalchemy.dialects.mysql import LONGBLOB

from airflow.migrations.db_types import TIMESTAMP, StringID

revision = "eed27faa34e3"
down_revision = "9fc3fc5de720"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Remove pickled data from xcom table."""
    # Summary of the change:
    # 1. Create an archived table (`_xcom_archive`) to store the current "pickled" data in the xcom table
    # 2. Extract and archive the pickled data using the condition
    # 3. Delete the pickled data from the xcom table so that we can update the column type
    # 4. Sanitize NaN values in JSON (convert to string)
    # 5. Update the XCom.value column type to JSON from LargeBinary/LongBlob

    conn = op.get_bind()
    dialect = conn.dialect.name

    # Create an archived table to store the current data
    op.create_table(
        "_xcom_archive",
        sa.Column("dag_run_id", sa.Integer(), nullable=False, primary_key=True),
        sa.Column("task_id", StringID(length=250), nullable=False, primary_key=True),
        sa.Column("map_index", sa.Integer(), nullable=False, server_default=sa.text("-1"), primary_key=True),
        sa.Column("key", StringID(length=512), nullable=False, primary_key=True),
        sa.Column("dag_id", StringID(length=250), nullable=False),
        sa.Column("run_id", StringID(length=250), nullable=False),
        sa.Column("value", sa.LargeBinary().with_variant(LONGBLOB, "mysql"), nullable=True),
        sa.Column("timestamp", TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("dag_run_id", "task_id", "map_index", "key"),
        if_not_exists=True,
    )

    # Condition to detect pickled data for different databases
    condition_templates = {
        "postgresql": "get_byte(value, 0) = 128",
        "mysql": "HEX(SUBSTRING(value, 1, 1)) = '80'",
        "sqlite": "hex(substr(value, 1, 1)) = '80'",
    }

    condition = condition_templates.get(dialect)
    if not condition:
        raise RuntimeError(f"Unsupported dialect: {dialect}")
    # Key is a reserved keyword in MySQL, so we need to quote it
    quoted_key = conn.dialect.identifier_preparer.quote("key")
    if dialect == "postgresql" and not context.is_offline_mode():
        curr_timeout = (
            int(
                conn.execute(
                    text("""
                        SELECT setting
                        FROM pg_settings
                        WHERE name = 'statement_timeout'
                    """)
                ).scalar_one()
            )
            / 1000
        )
        if curr_timeout > 0 and curr_timeout < 1800:
            print("setting local statement timeout to 1800s")
            conn.execute(text("SET LOCAL statement_timeout='1800s'"))

    # Archive pickled data using the condition
    conn.execute(
        text(
            f"""
            INSERT INTO _xcom_archive (dag_run_id, task_id, map_index, {quoted_key}, dag_id, run_id, value, timestamp)
            SELECT dag_run_id, task_id, map_index, {quoted_key}, dag_id, run_id, value, timestamp
            FROM xcom
            WHERE value IS NOT NULL AND {condition}
            """
        )
    )

    # Delete the pickled data from the xcom table so that we can update the column type
    conn.execute(text(f"DELETE FROM xcom WHERE value IS NOT NULL AND {condition}"))

    # Update the values from nan to nan string
    if dialect == "postgresql":
        # Replace NaN in JSON value positions (after :, , or [)
        # This explicitly matches JSON structure, not relying on word boundaries
        conn.execute(
            text(r"""
                UPDATE xcom
                SET value = convert_to(
                    regexp_replace(
                        convert_from(value, 'UTF8'),
                        '([:,\[]\s*)NaN(\s*[,}\]])',
                        '\1"nan"\2',
                        'g'
                    ),
                    'UTF8'
                )
                WHERE value IS NOT NULL AND get_byte(value, 0) != 128
            """)
        )

        op.execute(
            """
            ALTER TABLE xcom
            ALTER COLUMN value TYPE JSONB
            USING CASE
                WHEN value IS NOT NULL THEN CAST(CONVERT_FROM(value, 'UTF8') AS JSONB)
                ELSE NULL
            END
            """
        )
    elif dialect == "mysql":
        # Replace NaN in JSON value positions (after :, , or [)
        # Use alternation with proper grouping for MySQL compatibility
        conn.execute(
            text("""
                UPDATE xcom
                SET value = CONVERT(REGEXP_REPLACE(CONVERT(value USING utf8mb4), '[[:<:]]NaN[[:>:]]', '"nan"') USING BINARY)
                WHERE value IS NOT NULL AND HEX(SUBSTRING(value, 1, 1)) != '80'
            """)
        )

        op.add_column("xcom", sa.Column("value_json", sa.JSON(), nullable=True))
        op.execute("UPDATE xcom SET value_json = CAST(value AS CHAR CHARACTER SET utf8mb4)")
        op.drop_column("xcom", "value")
        op.alter_column("xcom", "value_json", existing_type=sa.JSON(), new_column_name="value")

    elif dialect == "sqlite":
        conn.execute(
            text("""
                UPDATE xcom
                SET value = CAST(REPLACE(CAST(value AS TEXT), 'NaN', '"nan"') AS BLOB)
                WHERE value IS NOT NULL AND hex(substr(value, 1, 1)) != '80'
            """)
        )
        # Rename the existing `value` column to `value_old`
        with op.batch_alter_table("xcom", schema=None) as batch_op:
            batch_op.alter_column("value", new_column_name="value_old")

        # Add the new `value` column with JSON type
        with op.batch_alter_table("xcom", schema=None) as batch_op:
            batch_op.add_column(sa.Column("value", sa.JSON(), nullable=True))

        # Migrate data from `value_old` to `value`
        conn.execute(
            text(
                """
                UPDATE xcom
                SET value = json(CAST(value_old AS TEXT))
                WHERE value_old IS NOT NULL
                """
            )
        )

        # Drop the old `value_old` column
        with op.batch_alter_table("xcom", schema=None) as batch_op:
            batch_op.drop_column("value_old")


def downgrade():
    """Unapply Remove pickled data from xcom table."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    # Revert the value column back to LargeBinary
    if dialect == "postgresql":
        op.execute(
            """
            ALTER TABLE xcom
            ALTER COLUMN value TYPE BYTEA
            USING CASE
                WHEN value IS NOT NULL THEN CONVERT_TO(value::TEXT, 'UTF8')
                ELSE NULL
            END
            """
        )
    elif dialect == "mysql":
        op.add_column("xcom", sa.Column("value_blob", LONGBLOB, nullable=True))
        op.execute("UPDATE xcom SET value_blob = CAST(value AS BINARY);")
        op.drop_column("xcom", "value")
        op.alter_column("xcom", "value_blob", existing_type=LONGBLOB, new_column_name="value")

    elif dialect == "sqlite":
        with op.batch_alter_table("xcom", schema=None) as batch_op:
            batch_op.alter_column("value", new_column_name="value_old")

        with op.batch_alter_table("xcom", schema=None) as batch_op:
            batch_op.add_column(sa.Column("value", sa.LargeBinary, nullable=True))

        conn.execute(
            text(
                """
                UPDATE xcom
                SET value = CAST(value_old AS BLOB)
                WHERE value_old IS NOT NULL
                """
            )
        )

        with op.batch_alter_table("xcom", schema=None) as batch_op:
            batch_op.drop_column("value_old")

    op.execute(sa.text("DROP TABLE IF EXISTS _xcom_archive"))
