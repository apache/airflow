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


# --- Value-sanitization SQL, factored out so migration tests can run the real statements
# against an isolated table; ``table`` defaults to "xcom" for the production calls.
#
# Two things round-trip through pickle but are illegal in strict JSON/JSONB:
#
# 1. The U+0000 (NUL) escape. JSONB cannot represent it and it cannot be quoted, so it is
#    stripped. Escaped backslashes are protected first so a literal ``\u0000`` in the data survives.
# 2. Non-finite floats (NaN / Infinity / -Infinity), rewritten to bare ``null``.
#
# Step 2 is skipped for values that already parse as JSON, where the token can only be inside a
# string: an XCom holding json.dumps'd JSON has its interior quotes escaped, so rewriting it
# would change data for no reason. Rows that do need it get ``null`` and not a quoted ``"NaN"``,
# because a raw quote is only valid at the top level. On a value that is invalid at the top level
# and also wraps an escaped document, the quote closes that string early and leaves the token
# bare, which aborts the cast.
#
# Step 1 runs first, or a value invalid only because of the escape looks like it needs step 2.
_XCOM_PG_STRIP_NUL_SQL = r"""
                UPDATE __TABLE__
                SET value = convert_to(
                    replace(
                        replace(
                            replace(convert_from(value, 'UTF8'), '\\', chr(1)),
                            '\u0000', ''
                        ),
                        chr(1), '\\'
                    ),
                    'UTF8'
                )
                WHERE value IS NOT NULL AND get_byte(value, 0) != 128
                    -- chr(1) never appears in the JSON text, json.dumps escapes control bytes.
                    -- The chain is an identity without the escape, so those rows are skipped
                    -- rather than rewritten.
                    AND position(convert_to('\u0000', 'UTF8') in value) > 0
            """
# PostgreSQL has no non-throwing JSON validator before 16 (``pg_input_is_valid``) and 14 is still
# supported, so the guard goes through a session-local function. The regex test comes first so
# the cast is only attempted for rows that have a token at all.
_XCOM_PG_NEEDS_NAN_FIX_SQL = r"""
                CREATE OR REPLACE FUNCTION pg_temp._airflow_xcom_needs_nan_fix(txt text)
                RETURNS boolean AS $$
                BEGIN
                    IF txt !~ '(NaN|Infinity)' THEN
                        RETURN false;
                    END IF;
                    PERFORM txt::jsonb;
                    RETURN false;
                EXCEPTION WHEN others THEN
                    RETURN true;
                END;
                $$ LANGUAGE plpgsql
            """
_XCOM_PG_SANITIZE_SQL = r"""
                UPDATE __TABLE__
                SET value = convert_to(
                    regexp_replace(
                        convert_from(value, 'UTF8'),
                        -- Group 1 is the preceding delimiter, or ^ for a bare scalar value. The
                        -- closing delimiter is a lookahead rather than a consuming group so that
                        -- consecutive tokens in an array ([NaN, Infinity]) each match. NaN and
                        -- Infinity share one pass to avoid a second table scan.
                        '([:,\[]\s*|^)(NaN|-?Infinity)(?=\s*[,}\]]|$)',
                        '\1null',
                        'g'
                    ),
                    'UTF8'
                )
                WHERE value IS NOT NULL AND get_byte(value, 0) != 128
                    AND pg_temp._airflow_xcom_needs_nan_fix(convert_from(value, 'UTF8'))
            """
_XCOM_MYSQL_STRIP_NUL_SQL = """
                UPDATE __TABLE__
                SET value = CONVERT(
                    REPLACE(
                        REPLACE(
                            REPLACE(CONVERT(value USING utf8mb4), '\\\\\\\\', CHAR(1)),
                            '\\\\u0000', ''
                        ),
                        CHAR(1), '\\\\\\\\'
                    ) USING BINARY
                )
                WHERE value IS NOT NULL AND HEX(SUBSTRING(value, 1, 1)) != '80'
                    AND LOCATE('\\\\u0000', value) > 0
            """
_XCOM_MYSQL_SANITIZE_SQL = """
                UPDATE __TABLE__
                SET value = CONVERT(
                    REGEXP_REPLACE(
                        CONVERT(value USING utf8mb4),
                        -- Same grouping and lookahead as PostgreSQL. The run of spaces after the
                        -- delimiter is inside group 1 so the replacement puts it back; outside the
                        -- group it was dropped, which is invisible in a top-level document but not
                        -- when the document is itself a JSON string. 'c' forces case-sensitive
                        -- matching (NaN != nan).
                        -- Python escaping: \\\\[ -> SQL \\[ -> regex \\[ -> literal [
                        '([:,\\\\[][ ]*|^)(NaN|-?Infinity)(?=[ ]*[,}\\\\]]|$)',
                        '$1null',
                        1,
                        0,
                        'c'
                    ) USING BINARY
                )
                WHERE value IS NOT NULL AND HEX(SUBSTRING(value, 1, 1)) != '80'
                    AND (LOCATE('NaN', value) > 0 OR LOCATE('Infinity', value) > 0)
                    AND NOT JSON_VALID(CONVERT(value USING utf8mb4))
            """
_XCOM_SQLITE_STRIP_NUL_SQL = """
                UPDATE __TABLE__
                SET value = CAST(
                    REPLACE(
                        REPLACE(
                            REPLACE(CAST(value AS TEXT), '\\\\', char(1)),
                            '\\u0000', ''
                        ),
                        char(1), '\\\\'
                    ) AS BLOB)
                WHERE value IS NOT NULL AND hex(substr(value, 1, 1)) != '80'
                    AND instr(CAST(value AS TEXT), '\\u0000') > 0
            """
# SQLite has no REGEXP_REPLACE, so this is a plain substring replace that cannot tell a token in
# a JSON syntax position from the same text inside a string. The json_valid() guard removes most
# of that risk: a value holding "NaN detected" still parses and is left alone. Only a value that
# fails to parse for some other reason can still be altered, and the result is valid JSON either
# way, so the migration completes. json_valid() needs the JSON1 extension, which SQLite builds
# before 3.38 may not have, so the guard is substituted in rather than inlined.
_XCOM_SQLITE_SANITIZE_SQL = """
                UPDATE __TABLE__
                SET value = CAST(
                    REPLACE(
                        REPLACE(
                            -- -Infinity first, or the bare Infinity step leaves '-null' behind.
                            REPLACE(CAST(value AS TEXT), '-Infinity', 'null'),
                            'Infinity', 'null'
                        ),
                        'NaN', 'null'
                    ) AS BLOB)
                WHERE value IS NOT NULL AND hex(substr(value, 1, 1)) != '80'
                    __GUARD__
            """
_SQLITE_JSON_VALID_GUARD = "AND NOT json_valid(CAST(value AS TEXT))"


def _xcom_pg_sanitize_statements(table: str = "xcom") -> list[str]:
    return [
        _XCOM_PG_STRIP_NUL_SQL.replace("__TABLE__", table),
        _XCOM_PG_NEEDS_NAN_FIX_SQL,
        _XCOM_PG_SANITIZE_SQL.replace("__TABLE__", table),
    ]


def _xcom_mysql_sanitize_statements(table: str = "xcom") -> list[str]:
    return [
        _XCOM_MYSQL_STRIP_NUL_SQL.replace("__TABLE__", table),
        _XCOM_MYSQL_SANITIZE_SQL.replace("__TABLE__", table),
    ]


def _xcom_sqlite_sanitize_statements(table: str = "xcom", json1: bool = True) -> list[str]:
    return [
        _XCOM_SQLITE_STRIP_NUL_SQL.replace("__TABLE__", table),
        _XCOM_SQLITE_SANITIZE_SQL.replace("__TABLE__", table).replace(
            "__GUARD__", _SQLITE_JSON_VALID_GUARD if json1 else ""
        ),
    ]


def _sqlite_has_json1(conn) -> bool:
    """Whether this SQLite build provides json_valid() (the JSON1 extension)."""
    try:
        conn.execute(text("SELECT json_valid('{}')")).fetchone()
    except Exception:
        print("SQLite JSON functions unavailable; sanitizing without the json_valid() guard.")
        return False
    return True


def upgrade():
    """Apply Remove pickled data from xcom table."""
    # Summary of the change:
    # 1. Create an archived table (`_xcom_archive`) to store the current "pickled" data in the xcom table
    # 2. Extract and archive the pickled data using the condition
    # 3. Delete the pickled data from the xcom table so that we can update the column type
    # 4. Sanitize values illegal in strict JSON/JSONB (strip the U+0000 NUL escape, null out NaN/Infinity)
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

    # Sanitize values that are legal in the pickled blob but illegal in strict JSON/JSONB
    # before changing the column type. See the statements at the top of this module.
    if dialect == "postgresql":
        for stmt in _xcom_pg_sanitize_statements():
            conn.execute(text(stmt))

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
        for stmt in _xcom_mysql_sanitize_statements():
            conn.execute(text(stmt))

        op.add_column("xcom", sa.Column("value_json", sa.JSON(), nullable=True))
        op.execute("UPDATE xcom SET value_json = CAST(value AS CHAR CHARACTER SET utf8mb4)")
        op.drop_column("xcom", "value")
        op.alter_column("xcom", "value_json", existing_type=sa.JSON(), new_column_name="value")

    elif dialect == "sqlite":
        for stmt in _xcom_sqlite_sanitize_statements(json1=_sqlite_has_json1(conn)):
            conn.execute(text(stmt))
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
