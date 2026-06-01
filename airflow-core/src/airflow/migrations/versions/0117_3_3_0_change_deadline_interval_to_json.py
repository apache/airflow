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
Change Deadline interval to JSON.

Revision ID: 8812eb67b63c
Revises: acc215baed80
Create Date: 2026-05-28 17:36:56.837243

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

# revision identifiers, used by Alembic.
revision = "8812eb67b63c"
down_revision = "acc215baed80"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Apply change deadline interval to JSON."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    if context.is_offline_mode():
        print(
            """
            Manual conversion required:

            PostgreSQL:

            Step 1: Convert column type.
            ALTER TABLE deadline_alert
            ALTER COLUMN interval TYPE JSONB
            USING to_json(interval);

            Step 2: Convert values.
            UPDATE deadline_alert
            SET interval = json_build_object(
                '__classname__', 'datetime.timedelta',
                '__version__', 2,
                '__data__', (interval::text)::float
            )
            WHERE jsonb_typeof(interval::jsonb) = 'number';

            MySQL:

            Step 1: Convert column type.
            ALTER TABLE deadline_alert MODIFY COLUMN `interval` JSON;

            Step 2: Convert values
            UPDATE deadline_alert
            SET `interval` = JSON_OBJECT(
                '__classname__', 'datetime.timedelta',
                '__version__', 2,
                '__data__', `interval`
            );

            SQLite:

            UPDATE deadline_alert
            SET interval =
                '{"__classname__":"datetime.timedelta","__version__":2,"__data__":'
                || CAST(interval AS TEXT) || '}';
            """
        )
        return

    with op.batch_alter_table("deadline_alert") as batch_op:
        if dialect == "postgresql":
            batch_op.alter_column(
                "interval",
                existing_type=sa.FLOAT(),
                type_=sa.JSON(),
                postgresql_using="to_json(interval)",
                existing_nullable=False,
            )
        else:
            batch_op.alter_column(
                "interval",
                existing_type=sa.FLOAT(),
                type_=sa.JSON(),
                existing_nullable=False,
            )

    if dialect == "postgresql":
        op.execute("""
            UPDATE deadline_alert
            SET interval = json_build_object(
                '__classname__', 'datetime.timedelta',
                '__version__', 2,
                '__data__', (interval::text)::float
            )
            WHERE jsonb_typeof(interval::jsonb) = 'number'
        """)

    elif dialect == "mysql":
        op.execute("""
            UPDATE deadline_alert
            SET `interval` = JSON_OBJECT(
                '__classname__', 'datetime.timedelta',
                '__version__', 2,
                '__data__', `interval`
            )
        """)

    else:
        op.execute("""
            UPDATE deadline_alert
            SET interval =
                '{"__classname__":"datetime.timedelta","__version__":'
                || '2' ||
                ',"__data__":' || CAST(interval AS TEXT) || '}'
            """)


def downgrade():
    """Revert deadline interval back to float."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    if context.is_offline_mode():
        print(
            """
            Manual downgrade required:

            PostgreSQL:

            Step 1: Convert values.
            UPDATE deadline_alert
            SET interval =
                CASE
                    WHEN jsonb_typeof(interval::jsonb) = 'number'
                        THEN interval
                    WHEN (interval::jsonb)->>'__classname__' = 'datetime.timedelta'
                        THEN to_json((interval->>'__data__')::double precision)
                    ELSE NULL
                END;

            Step 2: Convert column type.
            ALTER TABLE deadline_alert
            ALTER COLUMN interval TYPE DOUBLE PRECISION
            USING (
                CASE
                    WHEN jsonb_typeof(interval::jsonb) = 'number'
                        THEN interval::text::double precision
                    WHEN (interval::jsonb)->>'__classname__' = 'datetime.timedelta'
                        THEN (interval->>'__data__')::double precision
                    ELSE NULL
                END
            );

            MySQL:

            Step 1: Convert values
            UPDATE deadline_alert
            SET `interval` =
                CASE
                    WHEN JSON_EXTRACT(`interval`, '$.__data__') IS NOT NULL
                        THEN CAST(JSON_EXTRACT(`interval`, '$.__data__') AS DOUBLE)
                    WHEN JSON_EXTRACT(`interval`, '$.__classname__') IS NULL
                        THEN CAST(`interval` AS DOUBLE)
                    ELSE NULL
                END;

            Step 2: Convert column type
            ALTER TABLE deadline_alert
            MODIFY COLUMN `interval` DOUBLE;

            SQLite:

            Step 1: Convert values
            UPDATE deadline_alert
            SET interval =
                CASE
                    WHEN json_extract(interval, '$.__data__') IS NOT NULL
                    THEN CAST(json_extract(interval, '$.__data__') AS REAL)
                    WHEN json_extract(interval, '$.__classname__') IS NULL
                    THEN CAST(interval AS REAL)
                    ELSE NULL
                END;

            Step 2: SQLite does not support ALTER COLUMN TYPE.
            Recreate the table with interval as REAL and copy data.
            """
        )
        return

    if dialect == "postgresql":
        op.execute("""
            UPDATE deadline_alert
            SET interval =
                CASE
                    WHEN jsonb_typeof(interval::jsonb) = 'number'
                        THEN interval
                    WHEN (interval::jsonb)->>'__classname__' = 'datetime.timedelta'
                        THEN to_json((interval->>'__data__')::double precision)
                    ELSE NULL
                END
        """)

    elif dialect == "mysql":
        op.execute("""
            UPDATE deadline_alert
            SET `interval` =
                CASE
                    WHEN JSON_EXTRACT(`interval`, '$.__data__') IS NOT NULL
                    THEN CAST(JSON_EXTRACT(`interval`, '$.__data__') AS DOUBLE)
                    WHEN JSON_EXTRACT(`interval`, '$.__classname__') IS  NULL
                    THEN CAST(`interval` AS DOUBLE)
                    ELSE NULL
                END
        """)

    # Serialized VariableInterval objects do not contain a numeric "__data__" field
    # and therefore cannot be converted back to a float representation.
    # During downgrade, only timedelta-style serialized values are converted.
    # Other serialized interval types (e.g. VariableInterval) will cast as null.
    else:
        # Detect availability of SQLite JSON functions (JSON1 extension).
        json_functions_available = False
        try:
            conn.execute(sa.text("SELECT json_extract('{\"a\":1}', '$.a')")).fetchone()
            json_functions_available = True
        except Exception:
            print("SQLite JSON functions not available, using string parsing as fallback.")

        if json_functions_available:
            op.execute("""
                UPDATE deadline_alert
                SET interval =
                    CASE
                        WHEN json_extract(interval, '$.__data__') IS NOT NULL
                        THEN CAST(json_extract(interval, '$.__data__') AS REAL)
                        WHEN json_extract(interval, '$.__classname__') IS NULL
                        THEN CAST(interval AS REAL)
                        ELSE NULL
                    END
            """)
        else:
            # NOTE: This is a best-effort fallback for environments without JSON1.
            # It assumes a stable JSON format and may not work for all serialized values.
            op.execute("""
                UPDATE deadline_alert
                SET interval =
                    CASE
                        WHEN instr(interval, '__data__') > 0
                        THEN CAST(
                            substr(
                                interval,
                                instr(interval, '__data__') +
                                instr(substr(interval, instr(interval, '__data__')), ':')
                            ) AS FLOAT
                        )
                        WHEN instr(interval, '__classname__') = 0
                        THEN CAST(interval AS FLOAT)
                        ELSE NULL
                    END
                """)

    with op.batch_alter_table("deadline_alert") as batch_op:
        if dialect == "postgresql":
            batch_op.alter_column(
                "interval",
                existing_type=sa.JSON(),
                type_=sa.FLOAT(),
                postgresql_using="""
                CASE
                    WHEN jsonb_typeof(interval::jsonb) = 'number'
                        THEN interval::text::double precision
                    WHEN (interval::jsonb)->>'__classname__' = 'datetime.timedelta'
                        THEN (interval->>'__data__')::double precision
                    ELSE NULL
                END
                """,
                existing_nullable=False,
            )
        else:
            batch_op.alter_column(
                "interval",
                existing_type=sa.JSON(),
                type_=sa.FLOAT(),
                existing_nullable=False,
            )
