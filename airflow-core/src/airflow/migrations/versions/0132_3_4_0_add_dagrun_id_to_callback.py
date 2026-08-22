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
Add dagrun_id foreign key to callback.

Revision ID: 0f4c9a2b8e1d
Revises: c7f0a5d2e9b4
Create Date: 2026-08-06 00:00:00.000000

"""

from __future__ import annotations

import json

import sqlalchemy as sa
from alembic import context, op

from airflow.configuration import conf

# revision identifiers, used by Alembic.
revision = "0f4c9a2b8e1d"
down_revision = "c7f0a5d2e9b4"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def _deserialize_data(data):
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return {}
    return data.get("__var", data) if isinstance(data, dict) else {}


def _extract_dagrun_id(data) -> int | None:
    dagrun_id = _deserialize_data(data).get("dag_run_id")
    if dagrun_id is None:
        return None
    try:
        return int(dagrun_id)
    except (TypeError, ValueError):
        return None


def _backfill_dagrun_id_from_deadline(conn, batch_size: int) -> None:
    callback = sa.table(
        "callback",
        sa.column("id", sa.Uuid()),
        sa.column("type", sa.String(20)),
        sa.column("dagrun_id", sa.Integer()),
    )
    deadline = sa.table(
        "deadline",
        sa.column("callback_id", sa.Uuid()),
        sa.column("dagrun_id", sa.Integer()),
    )
    dag_run = sa.table("dag_run", sa.column("id", sa.Integer()))

    last_id = None
    while True:
        query = (
            sa.select(callback.c.id, deadline.c.dagrun_id)
            .select_from(
                callback.join(deadline, callback.c.id == deadline.c.callback_id).join(
                    dag_run, deadline.c.dagrun_id == dag_run.c.id
                )
            )
            .where(callback.c.type == "executor", callback.c.dagrun_id.is_(None))
            .order_by(callback.c.id)
            .limit(batch_size)
        )
        if last_id is not None:
            query = query.where(callback.c.id > last_id)

        rows = conn.execute(query).fetchall()
        if not rows:
            return

        for callback_id, dagrun_id in rows:
            conn.execute(callback.update().where(callback.c.id == callback_id).values(dagrun_id=dagrun_id))

        last_id = rows[-1].id


def _backfill_dagrun_id(conn, batch_size: int) -> None:
    callback = sa.table(
        "callback",
        sa.column("id", sa.Uuid()),
        sa.column("type", sa.String(20)),
        sa.column("data", sa.Text()),
        sa.column("dagrun_id", sa.Integer()),
    )
    dag_run = sa.table("dag_run", sa.column("id", sa.Integer()))

    last_id = None
    while True:
        query = (
            sa.select(callback.c.id, callback.c.data)
            .where(callback.c.type == "executor", callback.c.dagrun_id.is_(None))
            .order_by(callback.c.id)
            .limit(batch_size)
        )
        if last_id is not None:
            query = query.where(callback.c.id > last_id)

        rows = conn.execute(query).fetchall()
        if not rows:
            return

        for callback_id, data in rows:
            dagrun_id = _extract_dagrun_id(data)
            if dagrun_id is None:
                continue

            dagrun_exists = conn.execute(sa.select(dag_run.c.id).where(dag_run.c.id == dagrun_id)).first()
            if dagrun_exists:
                conn.execute(
                    callback.update().where(callback.c.id == callback_id).values(dagrun_id=dagrun_id)
                )

        last_id = rows[-1].id


def upgrade():
    """Add callback.dagrun_id and backfill it from legacy callback.data."""
    with op.batch_alter_table("callback") as batch_op:
        batch_op.add_column(sa.Column("dagrun_id", sa.Integer(), nullable=True))

    if not context.is_offline_mode():
        conn = op.get_bind()
        batch_size = conf.getint("database", "migration_batch_size")
        _backfill_dagrun_id_from_deadline(conn, batch_size)
        _backfill_dagrun_id(conn, batch_size)

    with op.batch_alter_table("callback") as batch_op:
        batch_op.create_index("callback_dagrun_id_idx", ["dagrun_id"], unique=False)
        batch_op.create_foreign_key(
            batch_op.f("callback_dagrun_id_fkey"), "dag_run", ["dagrun_id"], ["id"], ondelete="CASCADE"
        )


def downgrade():
    """Remove callback.dagrun_id."""
    with op.batch_alter_table("callback") as batch_op:
        batch_op.drop_constraint(batch_op.f("callback_dagrun_id_fkey"), type_="foreignkey")
        batch_op.drop_index("callback_dagrun_id_idx")
        batch_op.drop_column("dagrun_id")
