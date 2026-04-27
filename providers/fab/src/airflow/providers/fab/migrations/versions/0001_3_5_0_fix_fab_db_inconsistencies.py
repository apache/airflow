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
Fix fab db inconsistencies.

Revision ID: 02ca36b0235b
Revises: 6709f7a774b9
Create Date: 2026-03-10 14:07:31.559184

"""

from __future__ import annotations

import contextlib

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "02ca36b0235b"
down_revision = "6709f7a774b9"
branch_labels = None
depends_on = None
fab_version = "3.5.0"

# SQLite reflects inline FK/UQ constraints without names, which makes
# batch_op.drop_constraint(...) fail with "No such constraint". Passing this
# naming convention to batch_alter_table tells alembic to synthesize names
# for reflected unnamed constraints that match what batch_op.f() produces.
_naming_convention = {
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "uq": "%(table_name)s_%(column_0_N_name)s_uq",
}


def _mysql_run_procedure(procedure_name: str, body: str) -> str:
    return f"""
    DROP PROCEDURE IF EXISTS {procedure_name};
    CREATE PROCEDURE {procedure_name}()
    BEGIN
    {body}
    END;
    CALL {procedure_name}();
    DROP PROCEDURE IF EXISTS {procedure_name};
    """


def _mysql_create_idx_permission_view_id_if_not_exists() -> str:
    return _mysql_run_procedure(
        "CreateIdxPermissionViewId",
        """
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'ab_permission_view_role'
                AND INDEX_NAME = 'idx_permission_view_id'
        ) THEN
            CREATE INDEX `idx_permission_view_id` ON `ab_permission_view_role` (`permission_view_id`);
        END IF;
        """,
    )


def _mysql_create_idx_role_id_if_not_exists() -> str:
    return _mysql_run_procedure(
        "CreateIdxRoleId",
        """
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'ab_permission_view_role'
                AND INDEX_NAME = 'idx_role_id'
        ) THEN
            CREATE INDEX `idx_role_id` ON `ab_permission_view_role` (`role_id`);
        END IF;
        """,
    )


def _postgresql_drop_unique_constraints_on_ab_register_user_email() -> str:
    return """
    DO $$
    DECLARE r record;
    BEGIN
        FOR r IN
            SELECT DISTINCT tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = 'ab_register_user'
                AND tc.constraint_type = 'UNIQUE'
                AND kcu.column_name = 'email'
        LOOP
            EXECUTE 'ALTER TABLE ' || quote_ident('ab_register_user')
                || ' DROP CONSTRAINT IF EXISTS ' || quote_ident(r.constraint_name);
        END LOOP;
    END $$
    """


def _mysql_drop_unique_constraints_on_ab_register_user_email() -> str:
    return _mysql_run_procedure(
        "DropEmailUqIfExists",
        """
        DECLARE done INT DEFAULT FALSE;
        DECLARE v_name VARCHAR(255);
        DECLARE cur CURSOR FOR
            SELECT DISTINCT kcu.CONSTRAINT_NAME
            FROM information_schema.KEY_COLUMN_USAGE kcu
            JOIN information_schema.TABLE_CONSTRAINTS tc
                ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                AND kcu.TABLE_NAME = tc.TABLE_NAME
            WHERE kcu.TABLE_NAME = 'ab_register_user'
                AND kcu.TABLE_SCHEMA = DATABASE()
                AND tc.CONSTRAINT_TYPE = 'UNIQUE'
                AND kcu.COLUMN_NAME = 'email';
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        OPEN cur;
        drop_loop: LOOP
            FETCH cur INTO v_name;
            IF done THEN LEAVE drop_loop; END IF;
            SET @stmt = CONCAT('ALTER TABLE `ab_register_user` DROP INDEX `', v_name, '`');
            PREPARE s FROM @stmt;
            EXECUTE s;
            DEALLOCATE PREPARE s;
        END LOOP;
        CLOSE cur;
        """,
    )


def _drop_unique_constraint_if_exists(table_name: str, constraint_name: str) -> None:
    dialect_name = op.get_context().dialect.name

    if dialect_name == "postgresql":
        op.execute(sa.text(f'ALTER TABLE "{table_name}" DROP CONSTRAINT IF EXISTS "{constraint_name}"'))
    elif dialect_name == "mysql":
        op.execute(
            sa.text(
                _mysql_run_procedure(
                    "DropUniqueIfExists",
                    f"""
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.TABLE_CONSTRAINTS
                    WHERE
                        CONSTRAINT_SCHEMA = DATABASE() AND
                        TABLE_NAME = '{table_name}' AND
                        CONSTRAINT_NAME = '{constraint_name}' AND
                        CONSTRAINT_TYPE = 'UNIQUE'
                ) THEN
                    ALTER TABLE `{table_name}` DROP INDEX `{constraint_name}`;
                ELSE
                    SELECT 1;
                END IF;
                    """,
                )
            )
        )
    else:
        with op.batch_alter_table(table_name, schema=None) as batch_op:
            with contextlib.suppress(ValueError):
                batch_op.drop_constraint(constraint_name, type_="unique")


def _drop_index_if_exists(table_name: str, index_name: str) -> None:
    dialect_name = op.get_context().dialect.name

    if dialect_name == "mysql":
        op.execute(
            sa.text(
                _mysql_run_procedure(
                    "DropIndexIfExists",
                    f"""
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.STATISTICS
                    WHERE
                        TABLE_SCHEMA = DATABASE() AND
                        TABLE_NAME = '{table_name}' AND
                        INDEX_NAME = '{index_name}'
                ) THEN
                    DROP INDEX `{index_name}` ON `{table_name}`;
                END IF;
                    """,
                )
            )
        )
    else:
        op.drop_index(index_name, table_name=table_name, if_exists=True)


def upgrade() -> None:
    dialect_name = op.get_context().dialect.name
    bind = op.get_bind()
    if dialect_name == "postgresql":
        op.create_index(
            "idx_ab_user_username",
            "ab_user",
            [sa.literal_column("lower(username::text)")],
            unique=True,
            if_not_exists=True,
        )
        op.create_index(
            "idx_ab_register_user_username",
            "ab_register_user",
            [sa.literal_column("lower(username::text)")],
            unique=True,
            if_not_exists=True,
        )

    # These indexes exist in the ORM (models/__init__.py) so they may already be present
    # on ORM-created databases. Guard with an existence check for all paths including offline.
    if bind is not None:
        existing_pvr_indexes = {
            idx["name"] for idx in sa.inspect(bind).get_indexes("ab_permission_view_role")
        }
        if "idx_permission_view_id" not in existing_pvr_indexes:
            op.create_index("idx_permission_view_id", "ab_permission_view_role", ["permission_view_id"])
        if "idx_role_id" not in existing_pvr_indexes:
            op.create_index("idx_role_id", "ab_permission_view_role", ["role_id"])
    elif dialect_name == "postgresql":
        op.create_index(
            "idx_permission_view_id", "ab_permission_view_role", ["permission_view_id"], if_not_exists=True
        )
        op.create_index("idx_role_id", "ab_permission_view_role", ["role_id"], if_not_exists=True)
    elif dialect_name == "mysql":
        # Offline MySQL: CREATE INDEX IF NOT EXISTS is unsupported; use stored procedures.
        op.execute(sa.text(_mysql_create_idx_permission_view_id_if_not_exists()))
        op.execute(sa.text(_mysql_create_idx_role_id_if_not_exists()))

    with op.batch_alter_table(
        "ab_permission_view_role", schema=None, naming_convention=_naming_convention
    ) as batch_op:
        batch_op.drop_constraint(batch_op.f("ab_permission_view_role_role_id_fkey"), type_="foreignkey")
        batch_op.drop_constraint(
            batch_op.f("ab_permission_view_role_permission_view_id_fkey"), type_="foreignkey"
        )
        batch_op.create_foreign_key(
            batch_op.f("ab_permission_view_role_permission_view_id_fkey"),
            "ab_permission_view",
            ["permission_view_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            batch_op.f("ab_permission_view_role_role_id_fkey"),
            "ab_role",
            ["role_id"],
            ["id"],
            ondelete="CASCADE",
        )

    # Drop any existing unique constraint on email, regardless of its name.
    # Raw SQL is used so this works in both online and offline (--sql) mode.
    if dialect_name == "postgresql":
        op.execute(sa.text(_postgresql_drop_unique_constraints_on_ab_register_user_email()))
    elif dialect_name == "mysql":
        op.execute(sa.text(_mysql_drop_unique_constraints_on_ab_register_user_email()))
    elif dialect_name == "sqlite" and bind is not None:
        # SQLite: batch mode rewrites the table; requires a live connection.
        # Offline mode for SQLite is not supported by Airflow.
        for uq in sa.inspect(bind).get_unique_constraints("ab_register_user"):
            if "email" in uq["column_names"] and uq["name"] is not None:
                with op.batch_alter_table("ab_register_user", schema=None) as batch_op:
                    batch_op.drop_constraint(uq["name"], type_="unique")
    with op.batch_alter_table("ab_register_user", schema=None) as batch_op:
        batch_op.create_unique_constraint(batch_op.f("ab_register_user_email_uq"), ["email"])

    with op.batch_alter_table("ab_user_role", schema=None, naming_convention=_naming_convention) as batch_op:
        batch_op.drop_constraint(batch_op.f("ab_user_role_role_id_fkey"), type_="foreignkey")
        batch_op.drop_constraint(batch_op.f("ab_user_role_user_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("ab_user_role_role_id_fkey"), "ab_role", ["role_id"], ["id"], ondelete="CASCADE"
        )
        batch_op.create_foreign_key(
            batch_op.f("ab_user_role_user_id_fkey"), "ab_user", ["user_id"], ["id"], ondelete="CASCADE"
        )

    with op.batch_alter_table("ab_permission_view", schema=None) as batch_op:
        batch_op.alter_column("permission_id", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column("view_menu_id", existing_type=sa.INTEGER(), nullable=False)


def downgrade() -> None:
    dialect_name = op.get_context().dialect.name
    if dialect_name == "postgresql":
        op.drop_index("idx_ab_register_user_username", table_name="ab_register_user", if_exists=True)
        op.drop_index("idx_ab_user_username", table_name="ab_user", if_exists=True)

    _drop_unique_constraint_if_exists("ab_register_user", op.f("ab_register_user_email_uq"))

    if dialect_name == "mysql":
        with op.batch_alter_table("ab_permission_view_role", schema=None) as batch_op:
            batch_op.drop_constraint(batch_op.f("ab_permission_view_role_role_id_fkey"), type_="foreignkey")
            batch_op.drop_constraint(
                batch_op.f("ab_permission_view_role_permission_view_id_fkey"), type_="foreignkey"
            )
    _drop_index_if_exists("ab_permission_view_role", "idx_role_id")
    _drop_index_if_exists("ab_permission_view_role", "idx_permission_view_id")
    if dialect_name == "mysql":
        with op.batch_alter_table("ab_permission_view_role", schema=None) as batch_op:
            batch_op.create_foreign_key(
                batch_op.f("ab_permission_view_role_permission_view_id_fkey"),
                "ab_permission_view",
                ["permission_view_id"],
                ["id"],
                ondelete="CASCADE",
            )
            batch_op.create_foreign_key(
                batch_op.f("ab_permission_view_role_role_id_fkey"),
                "ab_role",
                ["role_id"],
                ["id"],
                ondelete="CASCADE",
            )

    with op.batch_alter_table("ab_permission_view", schema=None) as batch_op:
        batch_op.alter_column("view_menu_id", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column("permission_id", existing_type=sa.INTEGER(), nullable=True)
