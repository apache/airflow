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
Add UUID primary key to ``task_instance`` table.

Revision ID: d59cbbef95eb
Revises: 05234396c6fc
Create Date: 2024-10-21 22:39:12.394079
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d59cbbef95eb"
down_revision = "05234396c6fc"
branch_labels = "None"
depends_on = None
airflow_version = "3.0.0"

######
# The following functions to create UUID v7 are solely for the purpose of this migration.
# This is done for production databases that do not support UUID v7 natively (Postgres, MySQL)
# and used instead of uuids from
# python libraries like uuid6.uuid7() for performance reasons since the task_instance table
# can be very large.
######

# PostgreSQL-specific UUID v7 function
pg_uuid7_fn = """
DO $$
DECLARE
    pgcrypto_installed BOOLEAN;
BEGIN
    -- Check if pgcrypto is already installed
    pgcrypto_installed := EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto');

    -- Attempt to create pgcrypto if it is not installed
    IF NOT pgcrypto_installed THEN
        BEGIN
            CREATE EXTENSION pgcrypto;
            pgcrypto_installed := TRUE;
            RAISE NOTICE 'pgcrypto extension successfully created.';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'pgcrypto extension could not be installed due to insufficient privileges; using fallback';
                pgcrypto_installed := FALSE;
            WHEN OTHERS THEN
                RAISE NOTICE 'An unexpected error occurred while attempting to install pgcrypto; using fallback';
                pgcrypto_installed := FALSE;
        END;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION uuid_generate_v7(p_timestamp timestamp with time zone)
RETURNS uuid
LANGUAGE plpgsql
PARALLEL SAFE
AS $$
DECLARE
    unix_time_ms CONSTANT bytea NOT NULL DEFAULT substring(int8send((extract(epoch FROM p_timestamp) * 1000)::bigint) from 3);
    buffer bytea;
    pgcrypto_installed BOOLEAN := EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto');
BEGIN
    -- Use pgcrypto if available, otherwise use the fallback
    -- fallback from https://brandur.org/fragments/secure-bytes-without-pgcrypto
    IF pgcrypto_installed THEN
        buffer := unix_time_ms || gen_random_bytes(10);
    ELSE
        buffer := unix_time_ms || substring(uuid_send(gen_random_uuid()) FROM 1 FOR 5) ||
                  substring(uuid_send(gen_random_uuid()) FROM 12 FOR 5);
    END IF;

    -- Set UUID version and variant bits
    buffer := set_byte(buffer, 6, (b'0111' || get_byte(buffer, 6)::bit(4))::bit(8)::int);
    buffer := set_byte(buffer, 8, (b'10'   || get_byte(buffer, 8)::bit(6))::bit(8)::int);
    RETURN encode(buffer, 'hex')::uuid;
END
$$;
"""

pg_uuid7_fn_drop = """
DROP FUNCTION IF EXISTS uuid_generate_v7(timestamp with time zone);
"""

# MySQL-specific UUID v7 function
mysql_uuid7_fn = """
DROP FUNCTION IF EXISTS uuid_generate_v7;
CREATE FUNCTION uuid_generate_v7(p_timestamp DATETIME(3))
RETURNS CHAR(36)
DETERMINISTIC
BEGIN
    DECLARE unix_time_ms BIGINT;
    DECLARE time_hex CHAR(12);
    DECLARE rand_hex CHAR(24);
    DECLARE uuid CHAR(36);

    -- Convert the passed timestamp to milliseconds since epoch
    SET unix_time_ms = UNIX_TIMESTAMP(p_timestamp) * 1000;
    SET time_hex = LPAD(HEX(unix_time_ms), 12, '0');
    SET rand_hex = CONCAT(
        LPAD(HEX(FLOOR(RAND() * POW(2,32))), 8, '0'),
        LPAD(HEX(FLOOR(RAND() * POW(2,32))), 8, '0')
    );
    SET rand_hex = CONCAT(SUBSTRING(rand_hex, 1, 4), '7', SUBSTRING(rand_hex, 6));
    SET rand_hex = CONCAT(SUBSTRING(rand_hex, 1, 12), '8', SUBSTRING(rand_hex, 14));

    SET uuid = LOWER(CONCAT(
        SUBSTRING(time_hex, 1, 8), '-',
        SUBSTRING(time_hex, 9, 4), '-',
        SUBSTRING(rand_hex, 1, 4), '-',
        SUBSTRING(rand_hex, 5, 4), '-',
        SUBSTRING(rand_hex, 9)
    ));

    RETURN uuid;
END;
"""

mysql_uuid7_fn_drop = """
DROP FUNCTION IF EXISTS uuid_generate_v7;
"""

ti_table = "task_instance"

# Foreign key columns from task_instance
ti_fk_cols = ["dag_id", "task_id", "run_id", "map_index"]

# Foreign key constraints from other tables to task_instance
ti_fk_constraints = [
    {"table": "rendered_task_instance_fields", "fk": "rtif_ti_fkey"},
    {"table": "task_fail", "fk": "task_fail_ti_fkey"},
    {"table": "task_instance_history", "fk": "task_instance_history_ti_fkey"},
    {"table": "task_instance_note", "fk": "task_instance_note_ti_fkey"},
    {"table": "task_map", "fk": "task_map_task_instance_fkey"},
    {"table": "task_reschedule", "fk": "task_reschedule_ti_fkey"},
    {"table": "xcom", "fk": "xcom_task_instance_fkey"},
]


def _get_type_id_column(dialect_name: str) -> sa.types.TypeEngine:
    # For PostgreSQL, use the UUID type directly as it is more efficient
    if dialect_name == "postgresql":
        return postgresql.UUID(as_uuid=False)
    # For other databases, use String(36) to match UUID format
    else:
        return sa.String(36)


def upgrade():
    """Add UUID primary key to task instance table."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    op.add_column(
        "task_instance", sa.Column("id", _get_type_id_column(dialect_name), nullable=True)
    )

    if dialect_name == "postgresql":
        op.execute(pg_uuid7_fn)

        # TODO: Add batching to handle updates in smaller chunks for large tables to avoid locking
        # Migrate existing rows with UUID v7 using a timestamp-based generation
        op.execute(
            "UPDATE task_instance SET id = uuid_generate_v7(coalesce(queued_dttm, start_date, clock_timestamp()))"
        )

        op.execute(pg_uuid7_fn_drop)

        # Drop existing primary key constraint to task_instance table
        op.execute(
            "ALTER TABLE IF EXISTS task_instance DROP CONSTRAINT task_instance_pkey CASCADE"
        )

    elif dialect_name == "mysql":
        op.execute(mysql_uuid7_fn)

        # Migrate existing rows with UUID v7
        op.execute("""
            UPDATE task_instance
            SET id = uuid_generate_v7(coalesce(queued_dttm, start_date, NOW(3)))
            WHERE id IS NULL
        """)

        # Drop this function as it is no longer needed
        op.execute(mysql_uuid7_fn_drop)
        for fk in ti_fk_constraints:
            op.drop_constraint(fk["fk"], fk["table"], type_="foreignkey")
        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.drop_constraint("task_instance_pkey", type_="primary")
    elif dialect_name == "sqlite":
        from uuid6 import uuid7

        stmt = text("SELECT COUNT(*) FROM task_instance WHERE id IS NULL")
        conn = op.get_bind()
        task_instances = conn.execute(stmt).scalar()
        uuid_values = [str(uuid7()) for _ in range(task_instances)]

        # Ensure `uuid_values` is a list or iterable with the UUIDs for the update.
        stmt = text("""
            UPDATE task_instance
            SET id = :uuid
            WHERE id IS NULL
        """)

        for uuid_value in uuid_values:
            conn.execute(stmt.bindparams(uuid=uuid_value))

        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.drop_constraint("task_instance_pkey", type_="primary")

    # Add primary key and unique constraint to task_instance table
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.alter_column(
            "id", type_=_get_type_id_column(dialect_name), nullable=False
        )
        batch_op.create_unique_constraint("task_instance_composite_key", ti_fk_cols)
        batch_op.create_primary_key("task_instance_pkey", ["id"])

    # Create foreign key constraints
    for fk in ti_fk_constraints:
        with op.batch_alter_table(fk["table"]) as batch_op:
            batch_op.create_foreign_key(
                constraint_name=fk["fk"],
                referent_table=ti_table,
                local_cols=ti_fk_cols,
                remote_cols=ti_fk_cols,
                ondelete="CASCADE",
            )


def downgrade():
    """Drop UUID primary key to task instance table."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    if dialect_name == "postgresql":
        op.execute(
            "ALTER TABLE IF EXISTS task_instance DROP CONSTRAINT task_instance_composite_key CASCADE"
        )
        op.execute(pg_uuid7_fn_drop)

    elif dialect_name == "mysql":
        for fk in ti_fk_constraints:
            op.drop_constraint(fk["fk"], fk["table"], type_="foreignkey")

        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.drop_constraint("task_instance_composite_key", type_="unique")
        op.execute(mysql_uuid7_fn_drop)

    elif dialect_name == "sqlite":
        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.drop_constraint("task_instance_composite_key", type_="unique")

    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_constraint("task_instance_pkey", type_="primary")
        batch_op.drop_column("id")
        batch_op.create_primary_key("task_instance_pkey", ti_fk_cols)

    # Re-add foreign key constraints
    for fk in ti_fk_constraints:
        with op.batch_alter_table(fk["table"]) as batch_op:
            batch_op.create_foreign_key(
                constraint_name=fk["fk"],
                referent_table=ti_table,
                local_cols=ti_fk_cols,
                remote_cols=ti_fk_cols,
                ondelete="CASCADE",
            )
