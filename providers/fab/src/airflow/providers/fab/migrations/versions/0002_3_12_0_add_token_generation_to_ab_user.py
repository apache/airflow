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
Add token_generation to ab_user.

Revision ID: 20f2f9e27083
Revises: 02ca36b0235b
Create Date: 2026-11-18 02:34:33.000000

"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import sqlalchemy as sa
from alembic import op

revision = "20f2f9e27083"
down_revision = "02ca36b0235b"
branch_labels = None
depends_on = None
fab_version = "3.12.0"


@contextmanager
def _sqlite_rebuilds() -> Iterator[None]:
    """
    Run SQLite table rebuilds with foreign keys off, rolling back atomically on failure.

    Same as ``airflow.migrations.utils.sqlite_rebuilds``, which Airflow versions this provider
    supports do not all have. Dropping a column rebuilds ``ab_user`` on SQLite, and dropping the old
    table with foreign keys on cascades into ``ab_user_role``, deleting every role assignment.
    ``PRAGMA foreign_keys`` is ignored inside a transaction, hence the autocommit blocks.
    """
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
            op.execute(f"PRAGMA foreign_keys={1 if enabled else 0}")


def upgrade() -> None:
    """Add the token_generation column to ab_user."""
    with op.batch_alter_table("ab_user", schema=None) as batch_op:
        batch_op.add_column(sa.Column("token_generation", sa.Integer(), nullable=False, server_default="0"))


def downgrade() -> None:
    """Remove the token_generation column from ab_user."""
    with _sqlite_rebuilds(), op.batch_alter_table("ab_user", schema=None) as batch_op:
        batch_op.drop_column("token_generation")
