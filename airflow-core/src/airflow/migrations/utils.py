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

from alembic import op as alembic_op


def get_dialect_name(op) -> str:
    conn = op.get_bind()
    return conn.dialect.name if conn is not None else op.get_context().dialect.name


@contextmanager
def disable_sqlite_fkeys(op):
    if get_dialect_name(op) == "sqlite":
        with contextlib.ExitStack() as exit_stack:
            op.execute("PRAGMA foreign_keys=off")
            exit_stack.callback(op.execute, "PRAGMA foreign_keys=on")
            yield op
    else:
        yield op


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


def ignore_sqlite_value_error():
    if get_dialect_name(alembic_op) == "sqlite":
        return contextlib.suppress(ValueError)
    return contextlib.nullcontext()
