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

import json
import zlib
from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING

import sqlalchemy as sa
import uuid6
from alembic import context, op

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.serialization.enums import Encoding
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from typing import Any

    from sqlalchemy.engine import Connection

    ErrorDict = dict[str, list[str]]

revision = "55297ae24532"
down_revision = "e79fc784f145"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


CALLBACK_KEY = "callback_def"
DAG_KEY = "dag"
DEADLINE_KEY = "deadline"
INTERVAL_KEY = "interval"
REFERENCE_KEY = "reference"
DEADLINE_ALERT_REQUIRED_FIELDS = {REFERENCE_KEY, CALLBACK_KEY, INTERVAL_KEY}
DEFAULT_BATCH_SIZE = 1000
ENCODING_TYPE = "deadline_alert"


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
        sa.text("""
                SELECT reference, interval, callback_def
                FROM deadline_alert
                WHERE id = :alert_id
                """),
        {"alert_id": deadline_alert_id},
    ).fetchone()

    if not validation_result:
        print(f"ERROR: Failed to read back deadline_alert for DeadlineAlert {deadline_alert_id}")
        return False

    checks = [
        (REFERENCE_KEY, json.dumps(validation_result.reference, sort_keys=True), expected_reference),
        (INTERVAL_KEY, validation_result.interval, expected_interval),
        (CALLBACK_KEY, json.dumps(validation_result.callback_def, sort_keys=True), expected_callback),
    ]

    for name, actual, expected in checks:
        if actual != expected:
            print(f"ERROR: Written {name} does not match expected! Written: {actual}, Expected: {expected}")
            return False

    return True


def report_errors(errors: ErrorDict, operation: str = "migration") -> None:
    if errors:
        print(f"{len(errors)} Dags encountered errors: ")
        for dag_id, error in errors.items():
            print(f"  {dag_id}: {'; '.join(error)}")
    else:
        print(f"No Dags encountered errors during {operation}.")


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


def migrate_existing_deadline_alert_data_from_serialized_dag() -> None:
    """Extract DeadlineAlert data from serialized Dag data and populate deadline_alert table."""
    if context.is_offline_mode():
        print(
            """
            ------------
            --  WARNING: Unable to migrate DeadlineAlert data while in offline mode!
            --  The deadline_alert table will remain empty in offline mode.
            --  Run the migration in online mode to populate the deadline_alert table.
            ------------
            """
        )
        return

    BATCH_SIZE = conf.getint("database", "migration_batch_size", fallback=DEFAULT_BATCH_SIZE)

    processed_dags: list[str] = []
    dags_with_deadlines: set[str] = set()
    migrated_alerts_count: int = 0
    dags_with_errors: ErrorDict = defaultdict(list)
    batch_num = 0
    last_dag_id = ""

    conn = op.get_bind()
    dialect = conn.dialect.name

    total_dags = conn.execute(
        sa.text("SELECT COUNT(*) FROM serialized_dag WHERE data IS NOT NULL OR data_compressed IS NOT NULL")
    ).scalar()
    total_batches = (total_dags + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"Using migration_batch_size of {BATCH_SIZE} as set in Airflow configuration.")
    print(f"Starting migration of {total_dags} Dags in {total_batches} batches.\n")

    while True:
        batch_num += 1

        result = conn.execute(
            sa.text("""
                SELECT id, dag_id, data, data_compressed, created_at
                FROM serialized_dag
                WHERE (data IS NOT NULL OR data_compressed IS NOT NULL)
                  AND dag_id > :last_dag_id
                ORDER BY dag_id
                LIMIT :batch_size
            """),
            {"last_dag_id": last_dag_id, "batch_size": BATCH_SIZE},
        )

        batch_results = list(result)
        if not batch_results:
            break

        print(f"Processing batch {batch_num}...")

        for serialized_dag_id, dag_id, data, data_compressed, created_at in batch_results:
            processed_dags.append(dag_id)
            last_dag_id = dag_id

            # Create a savepoint for this Dag to allow rollback on error.
            savepoint = conn.begin_nested()

            try:
                dag_data = get_dag_data(data, data_compressed)

                if dag_deadline := dag_data[DAG_KEY][DEADLINE_KEY]:
                    dags_with_deadlines.add(dag_id)
                    deadline_alerts = dag_deadline if isinstance(dag_deadline, list) else [dag_deadline]

                    migrated_alert_ids = []

                    for serialized_alert in deadline_alerts:
                        if isinstance(serialized_alert, dict):
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

                                conn.execute(
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
                                    conn, deadline_alert_id, reference_data, interval_data, callback_data
                                ):
                                    dags_with_errors[dag_id].append(
                                        f"Invalid DeadlineAlert data: {serialized_alert}"
                                    )
                                    continue

                                migrated_alert_ids.append(deadline_alert_id)
                                migrated_alerts_count += 1

                                conn.execute(
                                    sa.text("""
                                        UPDATE deadline
                                        SET deadline_alert_id = :alert_id
                                        WHERE dagrun_id IN (
                                            SELECT dr.id
                                            FROM dag_run dr
                                                 JOIN serialized_dag sd ON dr.dag_id = sd.dag_id
                                            WHERE sd.id = :serialized_dag_id)
                                          AND deadline_alert_id IS NULL
                                    """),
                                    {"alert_id": deadline_alert_id, "serialized_dag_id": serialized_dag_id},
                                )
                            except Exception as e:
                                dags_with_errors[dag_id].append(f"Failed to process {serialized_alert}: {e}")
                                continue

                    if migrated_alert_ids:
                        uuid_strings = [str(uuid_id) for uuid_id in migrated_alert_ids]
                        update_dag_deadline_field(conn, serialized_dag_id, uuid_strings, dialect)

                        # Recalculate and update the dag_hash after modifying the deadline data to ensure
                        # it matches what write_dag() will compute later and avoid re-serialization.
                        updated_result = conn.execute(
                            sa.text(
                                "SELECT data, data_compressed "
                                "FROM serialized_dag "
                                "WHERE id = :serialized_dag_id"
                            ),
                            {"serialized_dag_id": serialized_dag_id},
                        ).fetchone()

                        if updated_result:
                            updated_dag_data = get_dag_data(
                                updated_result.data, updated_result.data_compressed
                            )
                            # Import here to avoid a circular dependency issue
                            new_hash = hash_dag(updated_dag_data)

                            conn.execute(
                                sa.text(
                                    "UPDATE serialized_dag "
                                    "SET dag_hash = :new_hash "
                                    "WHERE id = :serialized_dag_id"
                                ),
                                {"new_hash": new_hash, "serialized_dag_id": serialized_dag_id},
                            )

                # Commit the savepoint if everything succeeded for this Dag.
                savepoint.commit()

            except (json.JSONDecodeError, KeyError, TypeError) as e:
                dags_with_errors[dag_id].append(f"Could not process serialized Dag: {e}")
                savepoint.rollback()

        print(f"Batch {batch_num} of {total_batches} complete.")

    print(
        f"\nProcessed {len(processed_dags)} serialized_dag records ({len(set(processed_dags))} "
        f"unique Dags), {len(dags_with_deadlines)} had DeadlineAlerts."
    )
    print(f"Migrated {migrated_alerts_count} DeadlineAlert configurations.")
    report_errors(dags_with_errors, "migration")


def migrate_deadline_alert_data_back_to_serialized_dag() -> None:
    """Restore DeadlineAlert data from deadline_alert table back to serialized_dag."""
    from alembic import context

    if context.is_offline_mode():
        print(
            """
            ------------
            --  WARNING: Unable to restore DeadlineAlert data while in offline mode!
            --  The downgrade will skip data restoration in offline mode.
            --  Run the migration in online mode to restore the deadline_alert data.
            ------------
            """
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
    last_dag_id = ""

    conn = op.get_bind()
    dialect = conn.dialect.name

    # Count all dags - we'll filter in the loop for those with deadline data
    total_dags = conn.execute(
        sa.text("SELECT COUNT(*) FROM serialized_dag WHERE data IS NOT NULL OR data_compressed IS NOT NULL")
    ).scalar()

    total_batches = (total_dags + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"Using migration_batch_size of {BATCH_SIZE} as set in Airflow configuration.")
    print(f"Starting downgrade of {total_dags} Dags with DeadlineAlerts in {total_batches} batches.\n")

    while True:
        batch_num += 1

        result = conn.execute(
            sa.text("""
                    SELECT id, dag_id, data, data_compressed
                    FROM serialized_dag
                    WHERE (data IS NOT NULL OR data_compressed IS NOT NULL)
                      AND dag_id > :last_dag_id
                    ORDER BY dag_id
                    LIMIT :batch_size
                    """),
            {"last_dag_id": last_dag_id, "batch_size": BATCH_SIZE},
        )

        batch_results = list(result)
        if not batch_results:
            break
        print(f"Processing batch {batch_num}...")

        for serialized_dag_id, dag_id, data, data_compressed in batch_results:
            processed_dags.append(dag_id)
            last_dag_id = dag_id

            # Create a savepoint for this Dag to allow rollback on error.
            savepoint = conn.begin_nested()

            try:
                dag_data = get_dag_data(data, data_compressed)
                deadline_uuids = dag_data[DAG_KEY][DEADLINE_KEY]

                if not isinstance(deadline_uuids, list) or not deadline_uuids:
                    continue

                if not all(isinstance(uuid_val, str) for uuid_val in deadline_uuids):
                    print(f"WARNING: Dag {dag_id} has non-string deadline values, skipping")
                    continue

                dags_with_deadlines.add(dag_id)
                restored_deadline_objects = []

                alert_result = conn.execute(
                    sa.text("""
                            SELECT reference, interval, callback_def
                            FROM deadline_alert
                            WHERE serialized_dag_id = :serialized_dag_id
                            """),
                    {"serialized_dag_id": serialized_dag_id},
                ).fetchall()

                if not alert_result:
                    dags_with_errors[dag_id].append(
                        f"Could not find deadline_alert for serialized_dag {serialized_dag_id}"
                    )
                    continue

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

                # Replace the UUID array with the restored objects.
                if restored_deadline_objects:
                    update_dag_deadline_field(conn, serialized_dag_id, restored_deadline_objects, dialect)

                # Commit the savepoint if everything succeeded for this Dag.
                savepoint.commit()

            except Exception as e:
                dags_with_errors[dag_id].append(f"Could not restore deadline: {e}")
                savepoint.rollback()

        print(f"Batch {batch_num} of {total_batches} complete.")

    print(
        f"\nProcessed {len(processed_dags)} serialized_dag records ({len(set(processed_dags))} "
        f"unique Dags), {len(dags_with_deadlines)} had DeadlineAlerts."
    )
    print(f"Restored {restored_alerts_count} DeadlineAlert configurations to original format.")
    report_errors(dags_with_errors, "downgrade")
