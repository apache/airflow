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
Add required fields to enable UI integrations for the Deadline Alerts feature.

This migration creates the deadline_alert table to store DeadlineAlert definitions
and migrates existing Deadline Alert data from the serialized_dag JSON structure
into the new normalized table structure.

Revision ID: 55297ae24532
Revises: e79fc784f145
Create Date: 2025-10-17 16:04:55.016272
"""

from __future__ import annotations

import contextlib
import functools
import json
import zlib
from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog
import uuid6
from alembic import context, op

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.serialization.enums import Encoding
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.sqlalchemy import UtcDateTime

log = structlog.get_logger(__name__)


if TYPE_CHECKING:
    from typing import Any

    from sqlalchemy.engine import Connection

    ErrorDict = dict[str, list[str]]

revision = "55297ae24532"
down_revision = "e79fc784f145"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


CALLBACK_KEY = "callback"
DAG_KEY = "dag"
DEADLINE_KEY = "deadline"
INTERVAL_KEY = "interval"
REFERENCE_KEY = "reference"
DEADLINE_ALERT_REQUIRED_FIELDS = {REFERENCE_KEY, CALLBACK_KEY, INTERVAL_KEY}
DEFAULT_BATCH_SIZE = 1000
ENCODING_TYPE = "deadline_alert"


def _has_matching_index(conn: Connection, table_name: str, columns: Iterable[str]) -> bool:
    log.debug("Targeting index check", table=table_name, columns=list(columns))
    target_columns = list(columns)
    inspector = sa.inspect(conn)

    for index in inspector.get_indexes(table_name):
        index_columns = index.get("column_names") or []
        log.debug("Checking index", table=table_name, index=index.get("name"), columns=index_columns)
        if index_columns[: len(target_columns)] == target_columns:
            return True

    primary_key = inspector.get_pk_constraint(table_name) or {}
    primary_key_columns = primary_key.get("constrained_columns") or []
    log.info("Checking primary key", table=table_name, columns=primary_key_columns)
    if primary_key_columns[: len(target_columns)] == target_columns:
        return True

    with contextlib.suppress(Exception):
        for constraint in inspector.get_unique_constraints(table_name):
            constraint_columns = constraint.get("column_names") or []
            log.debug(
                "Checking unique constraint",
                table=table_name,
                constraint=constraint.get("name"),
                columns=constraint_columns,
            )
            if constraint_columns[: len(target_columns)] == target_columns:
                return True

    return False


def _is_mysql_foreign_key_index_error(conn: Connection, exc: sa.exc.OperationalError) -> bool:
    if conn.dialect.name != "mysql":
        return False

    error_args = getattr(exc.orig, "args", ())
    return bool(error_args) and error_args[0] == 1553


def temporary_index(index_name, table_name, columns):
    """Create an index before the wrapped function runs and drop it after, even on error."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            conn = op.get_bind()
            if _has_matching_index(conn, table_name, columns):
                log.info(
                    "Matching index already exists, skipping creation of temporary index",
                    index_name=index_name,
                    table_name=table_name,
                    columns=columns,
                )
                return func(*args, **kwargs)

            op.create_index(index_name, table_name, columns)
            try:
                return func(*args, **kwargs)
            finally:
                try:
                    op.drop_index(index_name, table_name=table_name)
                except sa.exc.OperationalError as exc:
                    if not _is_mysql_foreign_key_index_error(conn, exc):
                        raise
                    log.warning(
                        "Keeping temporary index because MySQL bound it to a foreign key",
                        index_name=index_name,
                        table_name=table_name,
                    )

        return wrapper

    return decorator


deadline_alert_table = sa.table(
    "deadline_alert",
    sa.column("reference"),
    sa.column("interval"),
    sa.column("callback_def"),
    sa.column("id"),
    sa.column("serialized_dag_id"),
)


def upgrade() -> None:
    """Make changes to enable adding DeadlineAlerts to the UI."""
    # TODO: We may finally have come up with a better naming convention. For ease of migration,
    #   we are going to keep deadline_alert here to match the model's name, but in the near future
    #   when this migration work is done we should deprecate the name DeadlineAlert (and all related
    #   classes, tables, etc) and replace it with DeadlineDefinition. Then we will have the
    #   user-provided DeadlineDefinition, and the actual instance of a Definition is (still) the Deadline.
    #   This feels more intuitive than DeadlineAlert defining the Deadline.

    op.create_table(
        "deadline_alert",
        sa.Column("id", sa.Uuid(), default=uuid6.uuid7),
        sa.Column("created_at", UtcDateTime, nullable=False),
        sa.Column("serialized_dag_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(250), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("reference", sa.JSON(), nullable=False),
        sa.Column("interval", sa.Float(), nullable=False),
        sa.Column("callback_def", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("deadline_alert_pkey")),
    )

    conn = op.get_bind()
    dialect_name = conn.dialect.name

    if dialect_name == "sqlite":
        conn.execute(sa.text("PRAGMA foreign_keys=OFF"))

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("deadline_alert_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("created_at", UtcDateTime, nullable=True))
        batch_op.add_column(sa.Column("last_updated_at", UtcDateTime, nullable=True))
        batch_op.create_foreign_key(
            batch_op.f("deadline_deadline_alert_id_fkey"),
            "deadline_alert",
            ["deadline_alert_id"],
            ["id"],
            ondelete="SET NULL",
        )

    # For migration/backcompat purposes if no timestamp is there from the migration, use now()
    # then lock the columns down so all new entries require the timestamps to be provided.
    now = timezone.utcnow()
    conn.execute(
        sa.text("""
            UPDATE deadline
            SET created_at = :now, last_updated_at = :now
            WHERE created_at IS NULL OR last_updated_at IS NULL
        """),
        {"now": now},
    )

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.alter_column("created_at", existing_type=UtcDateTime, nullable=False)
        batch_op.alter_column("last_updated_at", existing_type=UtcDateTime, nullable=False)

    with op.batch_alter_table("deadline_alert", schema=None) as batch_op:
        batch_op.create_foreign_key(
            batch_op.f("deadline_alert_serialized_dag_id_fkey"),
            "serialized_dag",
            ["serialized_dag_id"],
            ["id"],
            ondelete="CASCADE",
        )

    if dialect_name == "sqlite":
        conn.execute(sa.text("PRAGMA foreign_keys=ON"))

    migrate_existing_deadline_alert_data_from_serialized_dag()


def downgrade() -> None:
    """Remove changes that were added to enable adding DeadlineAlerts to the UI."""
    migrate_deadline_alert_data_back_to_serialized_dag()

    conn = op.get_bind()
    dialect_name = conn.dialect.name

    if dialect_name == "sqlite":
        conn.execute(sa.text("PRAGMA foreign_keys=OFF"))

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("deadline_deadline_alert_id_fkey"), type_="foreignkey")
        batch_op.drop_column("deadline_alert_id")
        batch_op.drop_column("last_updated_at")
        batch_op.drop_column("created_at")

    with op.batch_alter_table("deadline_alert", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("deadline_alert_serialized_dag_id_fkey"), type_="foreignkey")

    if dialect_name == "sqlite":
        conn.execute(sa.text("PRAGMA foreign_keys=ON"))

    op.drop_table("deadline_alert")


def get_dag_data(data: dict[str, Any] | str | None, data_compressed: bytes | None) -> dict[str, Any]:
    """
    Extract and decompress DAG data regardless of storage format.

    Returns the parsed JSON data, handling both compressed and uncompressed formats.

    :param data: The uncompressed DAG data as dict or JSON string.
    :param data_compressed: The compressed DAG data as bytes.
    """
    if data_compressed:
        decompressed = zlib.decompress(data_compressed)
        return json.loads(decompressed)
    return data if isinstance(data, dict) else json.loads(data)


def update_dag_deadline_field(
    conn: Connection,
    serialized_dag_id: str,
    deadline_data: list[str] | list[dict[str, Any]],
    dialect: str,
) -> None:
    """
    Update the deadline field in serialized_dag using the appropriate format.

    Checks the existing row to determine whether to use compressed or uncompressed format,
    and uses dialect-specific JSON operations when available.

    :param conn: SQLAlchemy database connection.
    :param serialized_dag_id: The serialized_dag.id identifier.
    :param deadline_data: List of deadline alert UUIDs (upgrade) or deadline objects (downgrade).
    :param dialect: Database dialect name (e.g., 'postgresql', 'mysql', 'sqlite').
    """
    check_compressed = conn.execute(
        sa.text("SELECT data_compressed FROM serialized_dag WHERE id = :serialized_dag_id"),
        {"serialized_dag_id": serialized_dag_id},
    ).fetchone()

    if check_compressed and check_compressed.data_compressed:
        decompressed = zlib.decompress(check_compressed.data_compressed)
        dag_data = json.loads(decompressed)
        dag_data[DAG_KEY][DEADLINE_KEY] = deadline_data
        new_compressed = zlib.compress(json.dumps(dag_data).encode("utf-8"))
        conn.execute(
            sa.text("UPDATE serialized_dag SET data_compressed = :data WHERE id = :serialized_dag_id"),
            {"data": new_compressed, "serialized_dag_id": serialized_dag_id},
        )
    elif dialect == "postgresql":
        conn.execute(
            sa.text("""
                    UPDATE serialized_dag
                    SET data = jsonb_set(
                        data::jsonb,
                        '{dag,deadline}',
                        CAST(:deadline_data AS jsonb)
                    )::json
                    WHERE id = :serialized_dag_id
                    """),
            {"serialized_dag_id": serialized_dag_id, "deadline_data": json.dumps(deadline_data)},
        )
    elif dialect == "mysql":
        conn.execute(
            sa.text("""
                    UPDATE serialized_dag
                    SET data = JSON_SET(
                        data,
                        '$.dag.deadline',
                        CAST(:deadline_data AS JSON)
                    )
                    WHERE id = :serialized_dag_id
                    """),
            {"serialized_dag_id": serialized_dag_id, "deadline_data": json.dumps(deadline_data)},
        )
    else:
        result = conn.execute(
            sa.text("SELECT data FROM serialized_dag WHERE id = :serialized_dag_id"),
            {"serialized_dag_id": serialized_dag_id},
        ).fetchone()

        if result and result.data:
            dag_data = json.loads(result.data) if isinstance(result.data, str) else result.data
            dag_data[DAG_KEY][DEADLINE_KEY] = deadline_data

            conn.execute(
                sa.text("UPDATE serialized_dag SET data = :data WHERE id = :serialized_dag_id"),
                {"data": json.dumps(dag_data), "serialized_dag_id": serialized_dag_id},
            )


def validate_written_data(
    conn: Connection,
    deadline_alert_id: str,
    expected_reference: str,
    expected_interval: float,
    expected_callback: str,
) -> bool:
    """
    Read back the inserted data and validate that it matches what we expect.

    This provides an extra safety check for data integrity during migration.

    :param conn: SQLAlchemy database connection.
    :param deadline_alert_id: The UUID of the deadline alert to validate.
    :param expected_reference: Expected JSON string of the reference field.
    :param expected_interval: Expected interval value as float.
    :param expected_callback: Expected JSON string of the callback field.
    """
    # TODO: Is this overkill?  Maybe... Consider adding a config option to
    #   disable validation for large deployments where performance is critical??

    validation_result = conn.execute(
        sa.select(
            deadline_alert_table.c.reference,
            deadline_alert_table.c.interval,
            deadline_alert_table.c.callback_def,
        ).where(deadline_alert_table.c.id == sa.bindparam("alert_id")),
        {"alert_id": deadline_alert_id},
    ).fetchone()

    if not validation_result:
        log.error("Failed to read back deadline_alert", deadline_alert_id=deadline_alert_id)
        return False

    checks = [
        (REFERENCE_KEY, json.dumps(validation_result.reference, sort_keys=True), expected_reference),
        (INTERVAL_KEY, validation_result.interval, expected_interval),
        (CALLBACK_KEY, json.dumps(validation_result.callback_def, sort_keys=True), expected_callback),
    ]

    for name, actual, expected in checks:
        if actual != expected:
            log.error("Written value does not match expected", field=name, actual=actual, expected=expected)
            return False

    return True


def report_errors(errors: ErrorDict, operation: str = "migration") -> None:
    if errors:
        log.warning("Dags encountered errors", operation=operation, count=len(errors), errors=dict(errors))
    else:
        log.info("No Dags encountered errors", operation=operation)


def hash_dag(dag_data):
    """
    Hash the data to get the dag_hash.

    Copied from airflow.models.serialized_dag.SerializedDagModel since we can't import it anymore.
    """
    dag_data = _sort_serialized_dag_dict(dag_data)
    data_ = dag_data.copy()
    # Remove fileloc from the hash so changes to fileloc
    # does not affect the hash. In 3.0+, a combination of
    # bundle_path and relative fileloc more correctly determines the
    # dag file location.
    data_["dag"].pop("fileloc", None)
    data_json = json.dumps(data_, sort_keys=True).encode("utf-8")
    return md5(data_json).hexdigest()


def _sort_serialized_dag_dict(serialized_dag: Any):
    """
    Recursively sort json_dict and its nested dictionaries and lists.

    Copied from airflow.models.serialized_dag.SerializedDagModel since we can't import it anymore.
    """
    if isinstance(serialized_dag, dict):
        return {k: _sort_serialized_dag_dict(v) for k, v in sorted(serialized_dag.items())}
    if isinstance(serialized_dag, list):
        if all(isinstance(i, dict) for i in serialized_dag):
            if all(
                isinstance(i.get("__var", {}), Iterable) and "task_id" in i.get("__var", {})
                for i in serialized_dag
            ):
                return sorted(
                    [_sort_serialized_dag_dict(i) for i in serialized_dag],
                    key=lambda x: x["__var"]["task_id"],
                )
        elif all(isinstance(item, str) for item in serialized_dag):
            return sorted(serialized_dag)
        return [_sort_serialized_dag_dict(i) for i in serialized_dag]
    return serialized_dag


@contextlib.contextmanager
def _begin_nested_transaction(conn):
    """
    Create a nested transaction.

    On SQLite, uses ``conn.begin_nested()`` with commit/rollback.
    On other backends, opens a new connection via ``conn.engine.begin()``
    and yields it so callers use the new connection for writes.
    """
    if conn.dialect.name != "sqlite":
        with conn.engine.begin() as new_conn:
            yield new_conn
        return
    try:
        savepoint = conn.begin_nested()
        yield conn
    except Exception:
        savepoint.rollback()
        raise
    savepoint.commit()


def migrate_existing_deadline_alert_data_from_serialized_dag() -> None:
    """Extract DeadlineAlert data from serialized Dag data and populate deadline_alert table."""
    if context.is_offline_mode():
        log.warning(
            "Unable to migrate DeadlineAlert data while in offline mode -- "
            "the deadline_alert table will remain empty. "
            "Run the migration in online mode to populate the deadline_alert table."
        )
        return

    _migrate_deadline_alerts()


# Temporary index on deadline.dagrun_id speeds up the per-alert UPDATE that finds
# matching deadline rows. Without it, every UPDATE full-scans the deadline table.
@temporary_index("tmp_deadline_dagrun_id_idx", "deadline", ["dagrun_id"])
def _migrate_deadline_alerts() -> None:
    BATCH_SIZE = conf.getint("database", "migration_batch_size", fallback=DEFAULT_BATCH_SIZE)

    processed_dags: list[str] = []
    dags_with_deadlines: set[str] = set()
    migrated_alerts_count: int = 0
    dags_with_errors: ErrorDict = defaultdict(list)
    batch_num = 0
    # Paginate by primary key (id) instead of dag_id because id is indexed (PK)
    # while dag_id has no index — using dag_id would cause a full table scan + sort
    # on every batch. UUID7 ids are time-ordered so the pagination order is stable.
    last_id = "00000000-0000-0000-0000-000000000000"

    conn = op.get_bind()
    dialect = conn.dialect.name

    # Build dialect-specific filter to skip rows without deadline data at the SQL level.
    # This avoids transferring and processing large data blobs for the majority of DAGs
    # that have no deadline configuration. Compressed rows are always included since the
    # DB cannot inspect their content.
    if dialect == "postgresql":
        deadline_filter = (
            "AND ("
            "  data_compressed IS NOT NULL"
            "  OR (data IS NOT NULL"
            "      AND (data::jsonb -> 'dag' -> 'deadline') IS NOT NULL"
            "      AND (data::jsonb -> 'dag' -> 'deadline') != 'null'::jsonb"
            "      AND (data::jsonb -> 'dag' -> 'deadline') != '[]'::jsonb)"
            ")"
        )
    elif dialect == "mysql":
        deadline_filter = (
            "AND ("
            "  data_compressed IS NOT NULL"
            "  OR (data IS NOT NULL"
            "      AND JSON_EXTRACT(data, '$.dag.deadline') IS NOT NULL"
            "      AND IFNULL(JSON_LENGTH(JSON_EXTRACT(data, '$.dag.deadline')), 0) > 0)"
            ")"
        )
    else:
        deadline_filter = (
            "AND ("
            "  data_compressed IS NOT NULL"
            "  OR (data IS NOT NULL"
            "      AND json_extract(data, '$.dag.deadline') IS NOT NULL"
            "      AND json_extract(data, '$.dag.deadline') != 'null'"
            "      AND json_extract(data, '$.dag.deadline') != '[]')"
            ")"
        )

    total_dags = conn.execute(
        sa.text("SELECT COUNT(*) FROM serialized_dag WHERE data IS NOT NULL OR data_compressed IS NOT NULL")
    ).scalar()
    total_batches = (total_dags + BATCH_SIZE - 1) // BATCH_SIZE

    log.info("Starting migration", batch_size=BATCH_SIZE, total_dags=total_dags, total_batches=total_batches)

    while True:
        batch_num += 1

        if dialect == "mysql":
            # Avoid selecting large columns during ORDER BY to prevent sort buffer overflow
            result = conn.execute(
                sa.text(f"""
                    SELECT sd.id, sd.dag_id, sd.data, sd.data_compressed, sd.created_at
                    FROM serialized_dag sd
                    INNER JOIN (
                        SELECT id
                        FROM serialized_dag
                        WHERE id > :last_id
                        {deadline_filter}
                        ORDER BY id
                        LIMIT :batch_size
                    ) AS subq ON sd.id = subq.id
                """),
                {"last_id": last_id, "batch_size": BATCH_SIZE},
            )

            batch_results = sorted(list(result), key=lambda r: r.id)
        else:
            result = conn.execute(
                sa.text(f"""
                    SELECT id, dag_id, data, data_compressed, created_at
                    FROM serialized_dag
                    WHERE id > :last_id
                      {deadline_filter}
                    ORDER BY id
                    LIMIT :batch_size
                """),
                {"last_id": last_id, "batch_size": BATCH_SIZE},
            )
            batch_results = list(result)
        if not batch_results:
            break

        log.info("Processing batch", batch_num=batch_num, total_batches=total_batches)

        for serialized_dag_id, dag_id, data, data_compressed, created_at in batch_results:
            processed_dags.append(dag_id)
            last_id = str(serialized_dag_id)

            # Validation that does not need a DB connection.
            try:
                dag_data = get_dag_data(data, data_compressed)
                dag_deadline = dag_data[DAG_KEY].get(DEADLINE_KEY)
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                dags_with_errors[dag_id].append(f"Could not process serialized Dag: {e}")
                continue
            if not dag_deadline:
                continue

            dags_with_deadlines.add(dag_id)
            deadline_alerts = dag_deadline if isinstance(dag_deadline, list) else [dag_deadline]

            def _migrate_dag_deadlines(dag_conn: Connection) -> Iterable[str]:
                dagrun_ids = [
                    row[0]
                    for row in dag_conn.execute(
                        sa.text("""
                            SELECT dr.id
                            FROM dag_run dr
                            JOIN serialized_dag sd ON dr.dag_id = sd.dag_id
                            WHERE sd.id = :serialized_dag_id
                        """),
                        {"serialized_dag_id": serialized_dag_id},
                    ).fetchall()
                ]
                for serialized_alert in deadline_alerts:
                    if not isinstance(serialized_alert, dict):
                        continue
                    try:
                        alert_data = serialized_alert.get(Encoding.VAR, serialized_alert)

                        if not DEADLINE_ALERT_REQUIRED_FIELDS.issubset(alert_data):
                            dags_with_errors[dag_id].append(
                                f"Invalid DeadlineAlert structure: {serialized_alert}"
                            )
                            continue

                        reference_data = json.dumps(alert_data[REFERENCE_KEY], sort_keys=True)
                        interval_data = float(alert_data.get(INTERVAL_KEY))
                        callback_data = json.dumps(alert_data[CALLBACK_KEY], sort_keys=True)
                        deadline_alert_id = str(uuid6.uuid7())

                        dag_conn.execute(
                            sa.text("""
                                    INSERT INTO deadline_alert (
                                        id,
                                        created_at,
                                        serialized_dag_id,
                                        reference,
                                        interval,
                                        callback_def,
                                        name,
                                        description)
                                    VALUES (
                                        :id,
                                        :created_at,
                                        :serialized_dag_id,
                                        :reference,
                                        :interval,
                                        :callback_def,
                                        NULL,
                                        NULL)
                                    """),
                            {
                                "id": deadline_alert_id,
                                "created_at": created_at or timezone.utcnow(),
                                "serialized_dag_id": serialized_dag_id,
                                "reference": reference_data,
                                "interval": interval_data,
                                "callback_def": callback_data,
                            },
                        )

                        if not validate_written_data(
                            dag_conn,
                            deadline_alert_id,
                            reference_data,
                            interval_data,
                            callback_data,
                        ):
                            dags_with_errors[dag_id].append(f"Invalid DeadlineAlert data: {serialized_alert}")
                            continue

                        yield deadline_alert_id
                        if dagrun_ids:
                            dag_conn.execute(
                                sa.text("""
                                    UPDATE deadline
                                    SET deadline_alert_id = :alert_id
                                    WHERE dagrun_id IN :dagrun_ids
                                        AND deadline_alert_id IS NULL
                                """).bindparams(sa.bindparam("dagrun_ids", expanding=True)),
                                {
                                    "alert_id": deadline_alert_id,
                                    "dagrun_ids": dagrun_ids,
                                },
                            )
                    except Exception as e:
                        dags_with_errors[dag_id].append(f"Failed to process {serialized_alert}: {e}")
                        continue

            try:
                with _begin_nested_transaction(conn) as dag_conn:
                    migrated_alert_ids = list(_migrate_dag_deadlines(dag_conn))
                    if not migrated_alert_ids:
                        continue

                    uuid_strings = [str(uuid_id) for uuid_id in migrated_alert_ids]

                    # Update the deadline field in the already-parsed dag_data and
                    # write back once, avoiding redundant decompression/recompression.
                    dag_data[DAG_KEY][DEADLINE_KEY] = uuid_strings
                    new_hash = hash_dag(dag_data)

                    if data_compressed:
                        new_compressed = zlib.compress(json.dumps(dag_data).encode("utf-8"))
                        dag_conn.execute(
                            sa.text(
                                "UPDATE serialized_dag "
                                "SET data_compressed = :data, dag_hash = :new_hash "
                                "WHERE id = :serialized_dag_id"
                            ),
                            {
                                "data": new_compressed,
                                "new_hash": new_hash,
                                "serialized_dag_id": serialized_dag_id,
                            },
                        )
                    elif dialect == "postgresql":
                        dag_conn.execute(
                            sa.text("""
                                UPDATE serialized_dag
                                SET data = jsonb_set(
                                    data::jsonb,
                                    '{dag,deadline}',
                                    CAST(:deadline_data AS jsonb)
                                )::json,
                                dag_hash = :new_hash
                                WHERE id = :serialized_dag_id
                            """),
                            {
                                "serialized_dag_id": serialized_dag_id,
                                "deadline_data": json.dumps(uuid_strings),
                                "new_hash": new_hash,
                            },
                        )
                    elif dialect == "mysql":
                        dag_conn.execute(
                            sa.text("""
                                UPDATE serialized_dag
                                SET data = JSON_SET(
                                    data,
                                    '$.dag.deadline',
                                    CAST(:deadline_data AS JSON)
                                ),
                                dag_hash = :new_hash
                                WHERE id = :serialized_dag_id
                            """),
                            {
                                "serialized_dag_id": serialized_dag_id,
                                "deadline_data": json.dumps(uuid_strings),
                                "new_hash": new_hash,
                            },
                        )
                    else:
                        dag_conn.execute(
                            sa.text(
                                "UPDATE serialized_dag "
                                "SET data = :data, dag_hash = :new_hash "
                                "WHERE id = :serialized_dag_id"
                            ),
                            {
                                "data": json.dumps(dag_data),
                                "new_hash": new_hash,
                                "serialized_dag_id": serialized_dag_id,
                            },
                        )
                    migrated_alerts_count += len(migrated_alert_ids)
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                log.exception("Could not migrate deadline for dag %s", dag_id)
                dags_with_errors[dag_id].append(f"Could not migrate deadline: {e}")

        log.info("Batch complete", batch_num=batch_num, total_batches=total_batches)

    log.info(
        "Migration complete",
        processed_records=len(processed_dags),
        unique_dags=len(set(processed_dags)),
        dags_with_deadlines=len(dags_with_deadlines),
        migrated_alerts=migrated_alerts_count,
    )
    report_errors(dags_with_errors, "migration")


def migrate_deadline_alert_data_back_to_serialized_dag() -> None:
    """Restore DeadlineAlert data from deadline_alert table back to serialized_dag."""
    from alembic import context

    if context.is_offline_mode():
        log.warning(
            "Unable to restore DeadlineAlert data while in offline mode -- "
            "the downgrade will skip data restoration. "
            "Run the migration in online mode to restore the deadline_alert data."
        )
        return

    from airflow.configuration import conf
    from airflow.serialization.enums import Encoding

    BATCH_SIZE = conf.getint("database", "migration_batch_size", fallback=DEFAULT_BATCH_SIZE)

    processed_dags: list[str] = []
    dags_with_deadlines: set[str] = set()
    restored_alerts_count: int = 0
    dags_with_errors: ErrorDict = defaultdict(list)
    batch_num = 0
    # Paginate by primary key (id) instead of dag_id because id is indexed (PK)
    # while dag_id has no index — using dag_id would cause a full table scan + sort
    # on every batch. UUID7 ids are time-ordered so the pagination order is stable.
    last_id = "00000000-0000-0000-0000-000000000000"

    conn = op.get_bind()
    dialect = conn.dialect.name

    # Count all dags - we'll filter in the loop for those with deadline data
    total_dags = conn.execute(
        sa.text("SELECT COUNT(*) FROM serialized_dag WHERE data IS NOT NULL OR data_compressed IS NOT NULL")
    ).scalar()

    total_batches = (total_dags + BATCH_SIZE - 1) // BATCH_SIZE

    log.info("Starting downgrade", batch_size=BATCH_SIZE, total_dags=total_dags, total_batches=total_batches)

    while True:
        batch_num += 1

        if dialect == "mysql":
            # Avoid selecting large columns during ORDER BY to prevent sort buffer overflow
            result = conn.execute(
                sa.text("""
                    SELECT sd.id, sd.dag_id, sd.data, sd.data_compressed
                    FROM serialized_dag sd
                    INNER JOIN (
                        SELECT id
                        FROM serialized_dag
                        WHERE (data IS NOT NULL OR data_compressed IS NOT NULL)
                          AND id > :last_id
                        ORDER BY id
                        LIMIT :batch_size
                    ) AS subq ON sd.id = subq.id
                    ORDER BY sd.id
                """),
                {"last_id": last_id, "batch_size": BATCH_SIZE},
            )
        else:
            result = conn.execute(
                sa.text("""
                    SELECT id, dag_id, data, data_compressed
                    FROM serialized_dag
                    WHERE (data IS NOT NULL OR data_compressed IS NOT NULL)
                      AND id > :last_id
                    ORDER BY id
                    LIMIT :batch_size
                """),
                {"last_id": last_id, "batch_size": BATCH_SIZE},
            )

        batch_results = list(result)
        if not batch_results:
            break
        log.info("Processing batch", batch_num=batch_num, total_batches=total_batches)

        for serialized_dag_id, dag_id, data, data_compressed in batch_results:
            processed_dags.append(dag_id)
            last_id = str(serialized_dag_id)

            # Validation that does not need a DB connection.
            try:
                dag_data = get_dag_data(data, data_compressed)
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
            deadline_uuids = (
                dag_data.get(DAG_KEY, {}).get(DEADLINE_KEY)
                if isinstance(dag_data.get(DAG_KEY), dict)
                else None
            )

            if not isinstance(deadline_uuids, list) or not deadline_uuids:
                continue

            if not all(isinstance(uuid_val, str) for uuid_val in deadline_uuids):
                log.warning("Dag has non-string deadline values, skipping", dag_id=dag_id)
                continue

            dags_with_deadlines.add(dag_id)

            try:
                with _begin_nested_transaction(conn) as dag_conn:
                    alert_result = dag_conn.execute(
                        sa.select(
                            deadline_alert_table.c.reference,
                            deadline_alert_table.c.interval,
                            deadline_alert_table.c.callback_def,
                        ).where(
                            deadline_alert_table.c.serialized_dag_id == sa.bindparam("serialized_dag_id")
                        ),
                        {"serialized_dag_id": serialized_dag_id},
                    ).fetchall()

                    if not alert_result:
                        dags_with_errors[dag_id].append(
                            f"Could not find deadline_alert for serialized_dag {serialized_dag_id}"
                        )
                        continue

                    restored_deadline_objects = []
                    for alert in alert_result:
                        deadline_object = {
                            Encoding.TYPE: ENCODING_TYPE,
                            Encoding.VAR: {
                                REFERENCE_KEY: alert.reference,
                                INTERVAL_KEY: float(alert.interval),
                                CALLBACK_KEY: alert.callback_def,
                            },
                        }
                        restored_deadline_objects.append(deadline_object)
                        restored_alerts_count += 1
                    if restored_deadline_objects:
                        update_dag_deadline_field(
                            dag_conn, serialized_dag_id, restored_deadline_objects, dialect
                        )
            except Exception as e:
                log.exception("Could not restore deadline for dag %s", dag_id)
                dags_with_errors[dag_id].append(f"Could not restore deadline: {e}")

        log.info("Batch complete", batch_num=batch_num, total_batches=total_batches)

    log.info(
        "Downgrade complete",
        processed_records=len(processed_dags),
        unique_dags=len(set(processed_dags)),
        dags_with_deadlines=len(dags_with_deadlines),
        restored_alerts=restored_alerts_count,
    )
    report_errors(dags_with_errors, "downgrade")
