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
Replace deadline's inline callback fields with foreign key to callback table.

Revision ID: e812941398f4
Revises: b87d2135fa50
Create Date: 2025-10-24 00:34:57.111239

"""

from __future__ import annotations

from datetime import datetime, timezone
from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy import column, select, table

from airflow.configuration import conf
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "e812941398f4"
down_revision = "b87d2135fa50"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


_CALLBACK_TYPE_TRIGGERER = "triggerer"
_CALLBACK_FETCH_METHOD_IMPORT_PATH = "import_path"
_CALLBACK_METRICS_PREFIX = "deadline_alerts"
_CALLBACK_STATE_PENDING = "pending"
_CALLBACK_STATE_SUCCESS = "success"
_CALLBACK_STATE_FAILED = "failed"
_ASYNC_CALLBACK_CLASSNAME = "airflow.sdk.definitions.deadline.AsyncCallback"


def _serialize_extended(value):
    """
    Encode ``value`` in Airflow's extended-JSON format, matching ``BaseSerialization.serialize``.

    ``callback.data`` is an ``ExtendedJSON`` column, so on read the runtime runs
    ``BaseSerialization.deserialize``, which requires every nested dict to be wrapped as
    ``{"__type": "dict", "__var": {...}}``. We must write that same wrapping here (recursively)
    rather than embedding raw nested dicts -- otherwise deserialize raises ``KeyError('__var')``
    and crashes the scheduler. The dict/list/primitive subset below is all that callback data
    contains; the logic is inlined so the migration stays independent of runtime serialization code.
    """
    if isinstance(value, dict):
        return {"__type": "dict", "__var": {str(k): _serialize_extended(v) for k, v in value.items()}}
    if isinstance(value, list):
        return [_serialize_extended(v) for v in value]
    return value


# Recursive extended-JSON encoder as a session-local (auto-dropped) SQL function, so the
# Postgres CTE path can wrap nested callback kwargs the same way ``_serialize_extended`` does.
_PG_ENCODE_EXTENDED_DDL = dedent("""
    CREATE OR REPLACE FUNCTION pg_temp.encode_extended(node jsonb) RETURNS jsonb AS $$
    DECLARE k text; v jsonb; out jsonb := '{}'::jsonb;
    BEGIN
      IF jsonb_typeof(node) = 'object' THEN
        FOR k, v IN SELECT * FROM jsonb_each(node) LOOP
          out := out || jsonb_build_object(k, pg_temp.encode_extended(v));
        END LOOP;
        RETURN jsonb_build_object('__type', 'dict', '__var', out);
      ELSIF jsonb_typeof(node) = 'array' THEN
        RETURN (SELECT jsonb_agg(pg_temp.encode_extended(e)) FROM jsonb_array_elements(node) e);
      END IF;
      RETURN node;
    END;
    $$ LANGUAGE plpgsql;
""")


def _deserialize_extended(value):
    """
    Inverse of :func:`_serialize_extended`: unwrap extended-JSON back to plain values.

    Used by ``downgrade`` to rebuild the old inline callback ``kwargs`` (which were stored
    raw). Lenient: already-raw dicts (e.g. produced by the pre-fix version of this migration)
    pass through unchanged, so downgrade is correct regardless of which version upgraded.
    """
    if isinstance(value, dict):
        if "__type" in value and "__var" in value:
            if value["__type"] == "dict" and isinstance(value["__var"], dict):
                return {k: _deserialize_extended(v) for k, v in value["__var"].items()}
            return value
        return {k: _deserialize_extended(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_deserialize_extended(v) for v in value]
    return value


# SQL inverse of ``pg_temp.encode_extended`` for the Postgres downgrade path. Lenient in the
# same way as ``_deserialize_extended`` so it handles data written by either version of upgrade.
_PG_DECODE_EXTENDED_DDL = dedent("""
    CREATE OR REPLACE FUNCTION pg_temp.decode_extended(node jsonb) RETURNS jsonb AS $$
    DECLARE k text; v jsonb; out jsonb := '{}'::jsonb;
    BEGIN
      IF jsonb_typeof(node) = 'object' THEN
        IF node ? '__type' AND node ? '__var' THEN
          IF node->>'__type' = 'dict' AND jsonb_typeof(node->'__var') = 'object' THEN
            FOR k, v IN SELECT * FROM jsonb_each(node->'__var') LOOP
              out := out || jsonb_build_object(k, pg_temp.decode_extended(v));
            END LOOP;
            RETURN out;
          END IF;
          RETURN node;
        ELSE
          FOR k, v IN SELECT * FROM jsonb_each(node) LOOP
            out := out || jsonb_build_object(k, pg_temp.decode_extended(v));
          END LOOP;
          RETURN out;
        END IF;
      ELSIF jsonb_typeof(node) = 'array' THEN
        RETURN (SELECT jsonb_agg(pg_temp.decode_extended(e)) FROM jsonb_array_elements(node) e);
      END IF;
      RETURN node;
    END;
    $$ LANGUAGE plpgsql;
""")


def _upgrade_postgresql(conn, batch_size):
    """Writable CTE per batch: SELECT window → INSERT callback → UPDATE deadline in one round-trip."""
    timestamp = datetime.now(timezone.utc)
    batch_num = 0
    last_id = "00000000-0000-0000-0000-000000000000"

    # Define the recursive encoder once; used to wrap nested kwargs in the CTE below.
    conn.execute(sa.text(_PG_ENCODE_EXTENDED_DDL))

    while True:
        batch_num += 1
        result = conn.execute(
            sa.text("""
                WITH batch AS (
                    SELECT
                        d.id AS deadline_id,
                        gen_random_uuid() AS callback_id,
                        COALESCE(dr.dag_id, '') AS dag_id,
                        COALESCE(d.callback::jsonb->'__data__'->>'path', '') AS cb_path,
                        COALESCE(NULLIF(d.callback::jsonb->'__data__'->'kwargs', 'null'::jsonb), '{}'::jsonb) AS cb_kwargs,
                        CASE
                            WHEN d.callback_state IN (:state_success, :state_failed) THEN d.callback_state
                            ELSE :state_pending
                        END AS cb_state,
                        COALESCE(d.callback_state IN (:state_success, :state_failed), FALSE) AS missed
                    FROM deadline d
                    LEFT JOIN dag_run dr ON d.dagrun_id = dr.id
                    WHERE d.id > :last_id
                    ORDER BY d.id
                    LIMIT :batch_size
                ),
                ins AS (
                    INSERT INTO callback (id, type, fetch_method, data, state, priority_weight, created_at)
                    SELECT
                        b.callback_id,
                        :cb_type,
                        :fetch_method,
                        json_build_object(
                            '__var', json_build_object(
                                'path', b.cb_path,
                                'kwargs', pg_temp.encode_extended(b.cb_kwargs),
                                'prefix', :prefix,
                                'dag_id', b.dag_id
                            ),
                            '__type', 'dict'
                        )::json,
                        b.cb_state,
                        1,
                        :ts
                    FROM batch b
                )
                UPDATE deadline d
                SET callback_id = b.callback_id,
                    missed = b.missed
                FROM batch b
                WHERE d.id = b.deadline_id
                RETURNING d.id
            """),
            {
                "last_id": last_id,
                "batch_size": batch_size,
                "state_success": _CALLBACK_STATE_SUCCESS,
                "state_failed": _CALLBACK_STATE_FAILED,
                "state_pending": _CALLBACK_STATE_PENDING,
                "cb_type": _CALLBACK_TYPE_TRIGGERER,
                "fetch_method": _CALLBACK_FETCH_METHOD_IMPORT_PATH,
                "prefix": _CALLBACK_METRICS_PREFIX,
                "ts": timestamp,
            },
        )

        rows = result.fetchall()
        row_count = len(rows)
        if row_count == 0:
            break

        last_id = str(max(r[0] for r in rows))
        print(f"Migrated {row_count} deadline records in batch {batch_num}")

        if row_count < batch_size:
            break

    remaining = conn.execute(
        sa.text("SELECT COUNT(*) FROM deadline WHERE missed IS NULL OR callback_id IS NULL")
    ).scalar()
    if remaining:
        print(f"WARNING: {remaining} deadline rows still have NULL missed/callback_id, fixing...")
        conn.execute(sa.text("UPDATE deadline SET missed = FALSE WHERE missed IS NULL"))


def _upgrade_mysql_sqlite(conn, batch_size):
    """Batched upgrade for MySQL/SQLite with Python UUID generation."""
    import json

    import uuid6

    timestamp = datetime.now(timezone.utc)

    deadline_table = table(
        "deadline",
        column("id", sa.Uuid()),
        column("dagrun_id", sa.Integer()),
        column("callback", sa.JSON()),
        column("callback_state", sa.String(20)),
        column("missed", sa.Boolean()),
        column("callback_id", sa.Uuid()),
    )

    dag_run_table = table(
        "dag_run",
        column("id", sa.Integer()),
        column("dag_id", sa.String(250)),
    )

    callback_table = table(
        "callback",
        column("id", sa.Uuid()),
        column("type", sa.String(20)),
        column("fetch_method", sa.String(20)),
        column("data", sa.JSON()),
        column("state", sa.String(10)),
        column("priority_weight", sa.Integer()),
        column("created_at", UtcDateTime(timezone=True)),
    )

    batch_num = 0
    null_callback_count = 0
    while True:
        batch_num += 1
        batch = conn.execute(
            select(
                deadline_table.c.id,
                deadline_table.c.callback,
                deadline_table.c.callback_state,
                dag_run_table.c.dag_id,
            )
            .outerjoin(dag_run_table, deadline_table.c.dagrun_id == dag_run_table.c.id)
            .where(deadline_table.c.callback_id.is_(None))
            .limit(batch_size)
        ).fetchall()

        if not batch:
            break

        callback_inserts = []
        deadline_updates = []

        for row in batch:
            callback_id = uuid6.uuid7()
            raw_cb = row.callback
            if raw_cb is None:
                null_callback_count += 1
                cb = {}
            elif isinstance(raw_cb, dict):
                cb = raw_cb
            else:
                cb = json.loads(raw_cb)
            cb_inner = cb.get("__data__", cb) if isinstance(cb, dict) else {}
            if not isinstance(cb_inner, dict):
                cb_inner = {}
            kwargs = cb_inner.get("kwargs", {})
            if not isinstance(kwargs, dict):
                kwargs = {}
            cb_data = {
                "path": cb_inner.get("path", "") or "",
                "kwargs": kwargs,
                "prefix": _CALLBACK_METRICS_PREFIX,
                "dag_id": row.dag_id or "",
            }

            if row.callback_state in (_CALLBACK_STATE_SUCCESS, _CALLBACK_STATE_FAILED):
                missed = True
                cb_state = row.callback_state
            else:
                missed = False
                cb_state = _CALLBACK_STATE_PENDING

            callback_inserts.append(
                {
                    "id": callback_id,
                    "type": _CALLBACK_TYPE_TRIGGERER,
                    "fetch_method": _CALLBACK_FETCH_METHOD_IMPORT_PATH,
                    "data": _serialize_extended(cb_data),
                    "state": cb_state,
                    "priority_weight": 1,
                    "created_at": timestamp,
                }
            )
            deadline_updates.append({"deadline_id": row.id, "callback_id": callback_id, "missed": missed})

        conn.execute(callback_table.insert(), callback_inserts)
        conn.execute(
            deadline_table.update()
            .where(deadline_table.c.id == sa.bindparam("deadline_id"))
            .values(callback_id=sa.bindparam("callback_id"), missed=sa.bindparam("missed")),
            deadline_updates,
        )
        print(f"Migrated {len(batch)} deadline records in batch {batch_num}")

    if null_callback_count:
        print(
            f"WARNING: {null_callback_count} deadline rows had NULL callback "
            "(legacy 0080 data); migrated with empty envelope."
        )


def upgrade():
    """Replace Deadline table's inline callback fields with callback_id foreign key."""
    with op.batch_alter_table("deadline") as batch_op:
        batch_op.add_column(sa.Column("missed", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("callback_id", sa.Uuid(), nullable=True))

    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  WARNING: Unable to migrate the data in the deadline table while in offline mode!
            --  All the rows in the deadline table will be deleted in this mode.
            ------------
            """)
        )
        op.execute("DELETE FROM deadline")
    else:
        conn = op.get_bind()
        batch_size = conf.getint("database", "migration_batch_size")
        if conn.dialect.name == "postgresql":
            _upgrade_postgresql(conn, batch_size)
        else:
            _upgrade_mysql_sqlite(conn, batch_size)

    with op.batch_alter_table("deadline") as batch_op:
        batch_op.alter_column("missed", existing_type=sa.Boolean(), nullable=False)
        batch_op.alter_column("callback_id", existing_type=sa.Uuid(), nullable=False)

        batch_op.create_index("deadline_missed_deadline_time_idx", ["missed", "deadline_time"], unique=False)
        batch_op.create_index("deadline_callback_id_idx", ["callback_id"], unique=False)
        batch_op.drop_index(batch_op.f("deadline_callback_state_time_idx"))
        batch_op.create_foreign_key(
            batch_op.f("deadline_callback_id_fkey"), "callback", ["callback_id"], ["id"], ondelete="CASCADE"
        )
        batch_op.drop_constraint(batch_op.f("deadline_trigger_id_fkey"), type_="foreignkey")
        batch_op.drop_column("callback")
        batch_op.drop_column("trigger_id")
        batch_op.drop_column("callback_state")


def _downgrade_postgresql(conn, batch_size):
    """Batched keyset pagination: bounded UPDATE ... FROM callback per batch."""
    batch_num = 0
    total_migrated = 0
    last_id = "00000000-0000-0000-0000-000000000000"

    # Define the inverse encoder once; used to unwrap nested kwargs back to raw below.
    conn.execute(sa.text(_PG_DECODE_EXTENDED_DDL))

    while True:
        batch_num += 1

        upper_row = conn.execute(
            sa.text("""
                SELECT id FROM deadline
                WHERE callback_id IS NOT NULL AND id > :last_id
                ORDER BY id
                OFFSET :offset LIMIT 1
            """),
            {"last_id": last_id, "offset": batch_size - 1},
        ).fetchone()

        if upper_row is None:
            result = conn.execute(
                sa.text("""
                    UPDATE deadline d
                    SET callback = json_build_object(
                            '__data__', json_build_object(
                                'path', c.data::jsonb->'__var'->>'path',
                                'kwargs', pg_temp.decode_extended(c.data::jsonb->'__var'->'kwargs')
                            ),
                            '__classname__', :classname,
                            '__version__', 0
                        )::json,
                        callback_state = CASE
                            WHEN c.state IN (:state_success, :state_failed) THEN c.state
                            ELSE NULL
                        END,
                        trigger_id = NULL,
                        callback_id = NULL
                    FROM callback c
                    WHERE c.id = d.callback_id
                      AND d.callback_id IS NOT NULL
                      AND d.id > :last_id
                """),
                {
                    "classname": _ASYNC_CALLBACK_CLASSNAME,
                    "state_success": _CALLBACK_STATE_SUCCESS,
                    "state_failed": _CALLBACK_STATE_FAILED,
                    "last_id": last_id,
                },
            )
            total_migrated += result.rowcount
            if result.rowcount > 0:
                print(f"Migrated {result.rowcount} deadline records in batch {batch_num} (final)")
            break

        upper_id = str(upper_row[0])
        result = conn.execute(
            sa.text("""
                UPDATE deadline d
                SET callback = json_build_object(
                        '__data__', json_build_object(
                            'path', c.data::jsonb->'__var'->>'path',
                            'kwargs', pg_temp.decode_extended(c.data::jsonb->'__var'->'kwargs')
                        ),
                        '__classname__', :classname,
                        '__version__', 0
                    )::json,
                    callback_state = CASE
                        WHEN c.state IN (:state_success, :state_failed) THEN c.state
                        ELSE NULL
                    END,
                    trigger_id = NULL,
                    callback_id = NULL
                FROM callback c
                WHERE c.id = d.callback_id
                  AND d.id > :last_id
                  AND d.id <= :upper_id
            """),
            {
                "classname": _ASYNC_CALLBACK_CLASSNAME,
                "state_success": _CALLBACK_STATE_SUCCESS,
                "state_failed": _CALLBACK_STATE_FAILED,
                "last_id": last_id,
                "upper_id": upper_id,
            },
        )
        total_migrated += result.rowcount
        last_id = upper_id
        print(f"Migrated {result.rowcount} deadline records in batch {batch_num}")

    print(f"Total migrated: {total_migrated} deadline records")


def _downgrade_mysql_sqlite(conn, batch_size):
    """Batched downgrade for MySQL/SQLite."""
    import json

    deadline_table = table(
        "deadline",
        column("id", sa.Uuid()),
        column("callback_id", sa.Uuid()),
        column("callback", sa.JSON()),
        column("callback_state", sa.String(20)),
        column("trigger_id", sa.Integer()),
    )

    callback_table = table(
        "callback",
        column("id", sa.Uuid()),
        column("data", sa.JSON()),
        column("state", sa.String(10)),
    )

    batch_num = 0
    while True:
        batch_num += 1
        batch = conn.execute(
            select(
                deadline_table.c.id.label("deadline_id"),
                deadline_table.c.callback_id,
                callback_table.c.data.label("callback_data"),
                callback_table.c.state.label("callback_state"),
            )
            .join(callback_table, deadline_table.c.callback_id == callback_table.c.id)
            .where(deadline_table.c.callback.is_(None))
            .limit(batch_size)
        ).fetchall()

        if not batch:
            break

        deadline_updates = []
        for row in batch:
            cb_data = (
                row.callback_data if isinstance(row.callback_data, dict) else json.loads(row.callback_data)
            )
            cb_inner = cb_data.get("__var", cb_data)

            callback_serialized = {
                "__data__": {
                    "path": cb_inner.get("path", ""),
                    "kwargs": _deserialize_extended(cb_inner.get("kwargs", {})),
                },
                "__classname__": _ASYNC_CALLBACK_CLASSNAME,
                "__version__": 0,
            }

            cb_state = (
                row.callback_state
                if row.callback_state in (_CALLBACK_STATE_SUCCESS, _CALLBACK_STATE_FAILED)
                else None
            )

            deadline_updates.append(
                {
                    "deadline_id": row.deadline_id,
                    "callback": callback_serialized,
                    "callback_state": cb_state,
                    "trigger_id": None,
                    "callback_id": None,
                }
            )

        conn.execute(
            deadline_table.update()
            .where(deadline_table.c.id == sa.bindparam("deadline_id"))
            .values(
                callback=sa.bindparam("callback"),
                callback_state=sa.bindparam("callback_state"),
                trigger_id=sa.bindparam("trigger_id"),
                callback_id=sa.bindparam("callback_id"),
            ),
            deadline_updates,
        )
        print(f"Migrated {len(batch)} deadline records in batch {batch_num}")


def downgrade():
    """Restore Deadline table's inline callback fields from callback_id foreign key."""
    with op.batch_alter_table("deadline") as batch_op:
        batch_op.add_column(sa.Column("callback_state", sa.VARCHAR(length=20), nullable=True))
        batch_op.add_column(sa.Column("trigger_id", sa.INTEGER(), autoincrement=False, nullable=True))
        batch_op.add_column(sa.Column("callback", sa.JSON(), nullable=True))
        batch_op.alter_column("callback_id", existing_type=sa.Uuid(), nullable=True)
        batch_op.drop_constraint(batch_op.f("deadline_callback_id_fkey"), type_="foreignkey")

    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  WARNING: Unable to migrate the data in the
            --  deadline and callback tables while in offline mode!
            --  All the rows in the deadline table and the referenced rows in
            --  the callback table will be deleted in this mode.
            ------------
            """)
        )
        op.execute("DELETE FROM deadline")
    else:
        conn = op.get_bind()
        batch_size = conf.getint("database", "migration_batch_size")
        if conn.dialect.name == "postgresql":
            _downgrade_postgresql(conn, batch_size)
        else:
            _downgrade_mysql_sqlite(conn, batch_size)

    op.execute(
        "DELETE FROM callback WHERE id NOT IN (SELECT callback_id FROM deadline WHERE callback_id IS NOT NULL)"
    )

    with op.batch_alter_table("deadline") as batch_op:
        batch_op.alter_column("callback", existing_type=sa.JSON(), nullable=False)
        batch_op.create_foreign_key(batch_op.f("deadline_trigger_id_fkey"), "trigger", ["trigger_id"], ["id"])
        batch_op.drop_index("deadline_missed_deadline_time_idx")
        batch_op.create_index(
            batch_op.f("deadline_callback_state_time_idx"), ["callback_state", "deadline_time"], unique=False
        )
        batch_op.drop_index("deadline_callback_id_idx")
        batch_op.drop_column("callback_id")
        batch_op.drop_column("missed")
