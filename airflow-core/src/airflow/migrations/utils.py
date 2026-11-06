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
from __future__ import annotations

import contextlib
from contextlib import contextmanager
from textwrap import dedent


@contextmanager
def disable_sqlite_fkeys(op):
    if op.get_bind().dialect.name == "sqlite":
        op.execute("PRAGMA foreign_keys=off")
        yield op
        op.execute("PRAGMA foreign_keys=on")
    else:
        yield op


# Unlike disable_sqlite_fkeys, a failed SQLite upgrade rolls back atomically and foreign_keys is always restored.
@contextmanager
def sqlite_rebuilds(op):
    if op.get_bind().dialect.name != "sqlite":
        yield
        return
    if op.get_context().as_sql:
        raise RuntimeError("SQLite offline SQL cannot render this migration's table rebuilds")
    enabled = op.get_bind().exec_driver_sql("PRAGMA foreign_keys").scalar()
    with op.get_context().autocommit_block():
        op.execute("PRAGMA foreign_keys=OFF")
    try:
        with op.get_bind().begin_nested():
            yield
    finally:
        with op.get_context().autocommit_block():
            op.execute(f"PRAGMA foreign_keys={int(enabled)}")


def mysql_drop_foreignkey_if_exists(constraint_name, table_name, op):
    """Older Mysql versions do not support DROP FOREIGN KEY IF EXISTS."""
    op.execute(f"""
    CREATE PROCEDURE DropForeignKeyIfExists()
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE
                CONSTRAINT_SCHEMA = DATABASE() AND
                TABLE_NAME = '{table_name}' AND
                CONSTRAINT_NAME = '{constraint_name}' AND
                CONSTRAINT_TYPE = 'FOREIGN KEY'
        ) THEN
            ALTER TABLE `{table_name}`
            DROP CONSTRAINT `{constraint_name}`;
        ELSE
            SELECT 1;
        END IF;
    END;
    CALL DropForeignKeyIfExists();
    DROP PROCEDURE DropForeignKeyIfExists;
    """)


def raise_if_rows_exist(select_sql: str, message: str, op) -> None:
    """
    Abort the migration when ``select_sql`` returns a row, using SQL so it also runs offline.

    Call it before any DDL: MySQL DDL is not transactional, so a late failure leaves a half-applied
    migration. ``message`` must not contain single quotes or percent signs and, for MySQL, must be at
    most 128 characters. PostgreSQL and MySQL only.
    """
    dialect = op.get_context().dialect.name
    if dialect == "postgresql":
        op.execute(
            dedent("""
                DO $$
                BEGIN
                    IF EXISTS ({select_sql}) THEN
                        RAISE EXCEPTION '{message}';
                    END IF;
                END $$
                """).format(select_sql=select_sql, message=message)
        )
    elif dialect == "mysql":
        create = (
            dedent("""
            CREATE PROCEDURE AirflowMigrationGuard()
            BEGIN
                IF EXISTS ({select_sql}) THEN
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{message}';
                END IF;
            END
            """)
            .strip()
            .format(select_sql=select_sql, message=message)
        )
        context = op.get_context()
        if context.as_sql:
            # The mysql client splits on ';' unless the procedure body is fenced by DELIMITER.
            context.output_buffer.write(
                "DROP PROCEDURE IF EXISTS AirflowMigrationGuard;\n"
                f"DELIMITER //\n{create}//\nDELIMITER ;\n"
                "CALL AirflowMigrationGuard();\nDROP PROCEDURE AirflowMigrationGuard;\n\n"
            )
        else:
            op.execute("DROP PROCEDURE IF EXISTS AirflowMigrationGuard")
            op.execute(create)
            try:
                op.execute("CALL AirflowMigrationGuard()")
            finally:
                op.execute("DROP PROCEDURE AirflowMigrationGuard")
    else:
        raise NotImplementedError(f"No SQL guard for dialect {dialect}")


def ignore_sqlite_value_error():
    from alembic import op

    if op.get_bind().dialect.name == "sqlite":
        return contextlib.suppress(ValueError)
    return contextlib.nullcontext()
