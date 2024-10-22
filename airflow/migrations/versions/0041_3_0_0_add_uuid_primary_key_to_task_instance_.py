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
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d59cbbef95eb"
down_revision = "05234396c6fc"
branch_labels = "None"
depends_on = None
airflow_version = "3.0.0"


# PostgreSQL-specific UUID v7 function
pg_uuid7_fn = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION
  uuid_generate_v7(p_timestamp timestamp with time zone)
RETURNS
  uuid
LANGUAGE
  plpgsql
PARALLEL SAFE
AS $$
  DECLARE
    unix_time_ms CONSTANT bytea NOT NULL DEFAULT substring(int8send((extract(epoch FROM p_timestamp) * 1000)::bigint) from 3);
    buffer bytea NOT NULL DEFAULT unix_time_ms || gen_random_bytes(10);
  BEGIN
    buffer = set_byte(buffer, 6, (b'0111' || get_byte(buffer, 6)::bit(4))::bit(8)::int);
    buffer = set_byte(buffer, 8, (b'10'   || get_byte(buffer, 8)::bit(6))::bit(8)::int);
    RETURN encode(buffer, 'hex');
  END
$$;
"""

pg_uuid7_fn_drop = """
DROP FUNCTION uuid_generate_v7(timestamp with time zone);
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
DROP FUNCTION uuid_generate_v7;
"""


def upgrade():
    """Add UUID primary key to task instance table."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    if dialect_name == "postgresql":
        op.add_column(
            "task_instance",
            sa.Column(
                "id",
                sa.String(length=32).with_variant(postgresql.UUID(), "postgresql"),
                nullable=True,
            ),
        )

        op.execute(pg_uuid7_fn)

        # TODO: Add batching to handle updates in smaller chunks for large tables to avoid locking
        # Migrate existing rows with UUID v7 using a timestamp-based generation
        op.execute(
            "UPDATE task_instance SET id = uuid_generate_v7(coalesce(queued_dttm, start_date, clock_timestamp()))"
        )
        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.alter_column("id", exiting_nullable=True, nullable=False)
            batch_op.create_index(
                "idx_ti_denormalized", ["task_id", "dag_id", "run_id", "map_index"], unique=True
            )

        # TODO: Convert the following into a Alembic operation
        # that way, we can capture the foreign key constraints which can be used
        # while downgrade too
        # So instead of cascading, we explicitly drop the constraint.
        op.execute("""
            ALTER TABLE task_instance
                DROP CONSTRAINT task_instance_pkey CASCADE,
                ADD CONSTRAINT task_instance_pkey PRIMARY KEY (id)
        """)

        # Drop the UUID v7 function after use
        op.execute(pg_uuid7_fn_drop)

    elif dialect_name == "mysql":
        op.add_column("task_instance", sa.Column("id", sa.String(length=36), nullable=True))
        # Apply the MySQL UUID v7 function
        op.execute(mysql_uuid7_fn)

        # Migrate existing rows with UUID v7
        op.execute("""
            UPDATE task_instance
            SET id = uuid_generate_v7(coalesce(queued_dttm, start_date, NOW(3)))
            WHERE id IS NULL
        """)

        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.alter_column("id", existing_type=sa.String(length=36), nullable=False)
            batch_op.create_index(
                "idx_ti_denormalized", ["task_id", "dag_id", "run_id", "map_index"], unique=True
            )

        # TODO: Add command to manually drop foreign key constraints
        # as MySQL does not support dropping foreign key constraints using CASCADE

        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.drop_constraint("task_instance_pkey", type_="primary")
            batch_op.create_primary_key("task_instance_pkey", ["id"])

        # Drop the MySQL UUID v7 function after use
        op.execute(mysql_uuid7_fn_drop)

    else:
        # TODO: Add support for Sqlite via the `uuid6` package using `uuid6.uuid7()`
        # https://pypi.org/project/uuid6/
        raise RuntimeError(f"TODO: Add support for {dialect_name} using the `uuid6` package")


def downgrade():
    """Drop UUID primary key to task instance table."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_constraint("task_instance_pkey", type_="primary")
        batch_op.create_primary_key("task_instance_pkey", ["task_id", "dag_id", "run_id", "map_index"])
        batch_op.drop_index("idx_ti_denormalized")
        batch_op.drop_column("id")

    # TODO: Re-add foreign key constraints
    if dialect_name == "postgresql":
        op.execute(pg_uuid7_fn_drop)

    elif dialect_name == "mysql":
        op.execute(mysql_uuid7_fn_drop)
