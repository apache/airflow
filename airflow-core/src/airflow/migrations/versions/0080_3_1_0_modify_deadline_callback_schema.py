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
Modify deadline's callback schema.

Revision ID: 808787349f22
Revises: 3bda03debd04
Create Date: 2025-07-31 19:35:53.150465

"""

from __future__ import annotations

import json
from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op

from airflow.configuration import conf

# revision identifiers, used by Alembic.
revision = "808787349f22"
down_revision = "3bda03debd04"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


_ASYNC_CALLBACK_CLASSNAME = "airflow.sdk.definitions.deadline.AsyncCallback"
# Maximum length of the callback VARCHAR column in the pre-0080 schema.
_CALLBACK_MAX_LEN = 500


def upgrade():
    """Replace deadline table's string callback and JSON callback_kwargs with JSON callback."""
    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  WARNING: Unable to migrate the data in the deadline table
            --  while in offline mode!  All rows in the deadline table will
            --  be deleted.
            ------------
            """)
        )
        op.execute("DELETE FROM deadline")
        with op.batch_alter_table("deadline", schema=None) as batch_op:
            batch_op.drop_column("callback")
            batch_op.drop_column("callback_kwargs")
            batch_op.add_column(sa.Column("callback", sa.JSON(), nullable=False))
        return

    conn = op.get_bind()
    batch_size = conf.getint("database", "migration_batch_size", fallback=1000)

    # Add the destination column alongside the existing ones so we can migrate
    # in batches without loading the whole table into memory at once.
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("callback_new", sa.JSON(), nullable=True))

    deadline_read = sa.table(
        "deadline",
        sa.column("id"),
        sa.column("callback"),
        sa.column("callback_kwargs", sa.JSON()),
        sa.column("callback_new", sa.JSON()),
    )
    deadline_write = sa.table(
        "deadline",
        sa.column("id"),
        sa.column("callback_new", sa.JSON()),
    )

    while True:
        rows = conn.execute(
            sa.select(
                deadline_read.c.id,
                deadline_read.c.callback,
                deadline_read.c.callback_kwargs,
            )
            .where(deadline_read.c.callback_new.is_(None))
            .limit(batch_size)
        ).fetchall()

        if not rows:
            break

        batch = []
        for row in rows:
            path = row[1] or ""
            kwargs = row[2]
            if isinstance(kwargs, str):
                kwargs = json.loads(kwargs) if kwargs else {}
            if not isinstance(kwargs, dict):
                kwargs = {}
            batch.append(
                {
                    "row_id": row[0],
                    "new_callback": {
                        "__data__": {"path": path, "kwargs": kwargs},
                        "__classname__": _ASYNC_CALLBACK_CLASSNAME,
                        "__version__": 0,
                    },
                }
            )

        conn.execute(
            sa.update(deadline_write)
            .where(deadline_write.c.id == sa.bindparam("row_id"))
            .values(callback_new=sa.bindparam("new_callback")),
            batch,
        )

        if len(rows) < batch_size:
            break

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_column("callback")
        batch_op.drop_column("callback_kwargs")
        batch_op.alter_column(
            "callback_new",
            new_column_name="callback",
            existing_type=sa.JSON(),
            nullable=False,
        )


def downgrade():
    """Replace deadline table's JSON callback with string callback and JSON callback_kwargs."""
    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  WARNING: Unable to migrate the data in the deadline table
            --  while in offline mode!  All rows in the deadline table will
            --  be deleted.
            ------------
            """)
        )
        op.execute("DELETE FROM deadline")
        with op.batch_alter_table("deadline", schema=None) as batch_op:
            batch_op.drop_column("callback")
            batch_op.add_column(sa.Column("callback_kwargs", sa.JSON(), nullable=True))
            batch_op.add_column(sa.Column("callback", sa.String(length=500), nullable=False))
        return

    conn = op.get_bind()
    batch_size = conf.getint("database", "migration_batch_size", fallback=1000)

    # Add the restored columns alongside the existing JSON callback so we can
    # back-fill in batches before dropping the JSON column.
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("callback_old", sa.String(length=500), nullable=True))
        batch_op.add_column(sa.Column("callback_kwargs", sa.JSON(), nullable=True))

    deadline_read = sa.table(
        "deadline",
        sa.column("id"),
        sa.column("callback", sa.JSON()),
        sa.column("callback_old", sa.String(500)),
    )
    deadline_write = sa.table(
        "deadline",
        sa.column("id"),
        sa.column("callback_old", sa.String(500)),
        sa.column("callback_kwargs", sa.JSON()),
    )

    while True:
        rows = conn.execute(
            sa.select(deadline_read.c.id, deadline_read.c.callback)
            .where(deadline_read.c.callback_old.is_(None))
            .limit(batch_size)
        ).fetchall()

        if not rows:
            break

        batch = []
        for row in rows:
            cb = row[1]
            if cb is None:
                path, kwargs = "", {}
            else:
                if isinstance(cb, str):
                    cb = json.loads(cb)
                cb_inner = cb.get("__data__", cb) if isinstance(cb, dict) else {}
                path = cb_inner.get("path", "")
                if len(path) > _CALLBACK_MAX_LEN:
                    print(
                        f"WARNING: callback path for deadline {row[0]} exceeds "
                        f"{_CALLBACK_MAX_LEN} chars and will be truncated."
                    )
                    path = path[:_CALLBACK_MAX_LEN]
                kwargs = cb_inner.get("kwargs", {})
                if not isinstance(kwargs, dict):
                    print(
                        f"WARNING: kwargs for deadline {row[0]} is not a dict "
                        f"(type={type(kwargs).__name__}); resetting to empty dict."
                    )
                    kwargs = {}
            batch.append({"row_id": row[0], "old_callback": path, "old_kwargs": kwargs})

        conn.execute(
            sa.update(deadline_write)
            .where(deadline_write.c.id == sa.bindparam("row_id"))
            .values(
                callback_old=sa.bindparam("old_callback"),
                callback_kwargs=sa.bindparam("old_kwargs"),
            ),
            batch,
        )

        if len(rows) < batch_size:
            break

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_column("callback")
        batch_op.alter_column(
            "callback_old",
            new_column_name="callback",
            existing_type=sa.String(500),
            nullable=False,
        )
