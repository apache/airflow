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

Revision ID: 55297ae24532
Revises: b87d2135fa50
Create Date: 2025-10-17 16:04:55.016272
"""

from __future__ import annotations

import json

import sqlalchemy as sa
import uuid6
from alembic import op
from sqlalchemy_utils import UUIDType

from airflow._shared.timezones import timezone
from airflow.migrations.db_types import TIMESTAMP
from airflow.models import ID_LEN
from airflow.serialization.enums import Encoding

from airflow.utils.sqlalchemy import UtcDateTime

revision = "55297ae24532"
down_revision = "b87d2135fa50"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"

#  TODO:  does this even work if they have data_compressed on???

def upgrade():
    """Make changes to enable adding DeadlineAlerts to the UI."""
    # TODO:   We may finally have come up with a better naming convention. For ease of migration,
    #   we are going to keep deadline_alert here to match the model's name, but in the near future
    #   when this migration work is done we should deprecate the name DeadlineAlert (and all related
    #   classes, tables, etc) and replace it with DeadlineDefinition.  Then we will have the
    #   user-provided DeadlineDefinition, and the actual instance of a Definition is (still) the Deadline.
    #   This feels more intuitive than DeadlineAlert defining the Deadline.

    op.create_table(
        "deadline_alert",
        sa.Column("id", UUIDType(binary=False), default=uuid6.uuid7),
        sa.Column(
            "created_at", UtcDateTime, nullable=False, server_default=sa.text("timezone('utc', now())")
        ),
        sa.Column("dag_id", sa.String(ID_LEN), nullable=False),
        sa.Column("name", sa.String(250), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("reference", sa.JSON(), nullable=False),
        sa.Column("interval", sa.Integer(), nullable=False),
        sa.Column("callback", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("deadline_alert_pkey")),
    )

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("deadline_alert_id", UUIDType(binary=False), nullable=True))
        batch_op.add_column(
            sa.Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now())
        )
        batch_op.add_column(
            sa.Column(
                "last_updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()
            )
        )

    op.create_foreign_key(
        op.f("deadline_deadline_alert_id_fkey"),
        "deadline",
        "deadline_alert",
        ["deadline_alert_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_foreign_key(
        op.f("deadline_alert_dag_id_fkey"),
        "deadline_alert",
        "dag",
        ["dag_id"],
        ["dag_id"],
        ondelete="CASCADE",
    )

    migrate_existing_deadline_alert_data_from_serialized_dag()


def downgrade():
    """Remove changes that were added to enable adding DeadlineAlerts to the UI."""

    migrate_deadline_alert_data_back_to_serialized_dag()

    op.drop_constraint(op.f("deadline_deadline_alert_id_fkey"), "deadline", type_="foreignkey")
    op.drop_constraint(op.f("deadline_alert_dag_id_fkey"), "deadline_alert", type_="foreignkey")

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_column("deadline_alert_id", if_exists=True)
        batch_op.drop_column("last_updated_at", if_exists=True)
        batch_op.drop_column("created_at", if_exists=True)

    op.drop_table("deadline_alert")


def migrate_existing_deadline_alert_data_from_serialized_dag():
    """Extract DeadlineAlert data from serialized DAG data and populate deadline_alert table."""
    import json

    import uuid6

    from airflow.configuration import conf

    BATCH_SIZE = conf.getint("database", "migration_batch_size", fallback=1000)

    processed_dags_count: int = 0
    dags_with_deadlines_count: int = 0
    migrated_alerts_count: int = 0
    dags_with_errors: set[str] = set()
    batch_num = 0
    last_dag_id = ""

    conn = op.get_bind()

    total_dags = conn.execute(sa.text("SELECT COUNT(*) FROM serialized_dag WHERE data IS NOT NULL")).scalar()
    total_batches = (total_dags + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"Using migration_batch_size of {BATCH_SIZE} as set in Airflow configuration.")
    print(f"Starting migration of {total_dags} DAGs in {total_batches} batches.")

    while True:
        batch_num += 1

        result = conn.execute(
            sa.text("""
                SELECT dag_id, data, created_at
                FROM serialized_dag
                WHERE data IS NOT NULL
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

        for dag_id, data, created_at in batch_results:
            processed_dags_count += 1
            last_dag_id = dag_id

            try:
                if dag_deadline := data.get("dag", {}).get("deadline", None):
                    dags_with_deadlines_count += 1
                    deadline_alerts = dag_deadline if isinstance(dag_deadline, list) else [dag_deadline]

                    for serialized_alert in deadline_alerts:
                        if isinstance(serialized_alert, dict):
                            try:
                                alert_data = serialized_alert.get(Encoding.VAR, serialized_alert)

                                if not set(['reference', 'callback', 'interval']).issubset(alert_data):
                                    print(f"\n\nWARNING: Invalid deadline alert structure in DAG {dag_id}: {serialized_alert}\n\n")
                                    dags_with_errors.add(dag_id)
                                    continue

                                reference_data = json.dumps(alert_data['reference'], sort_keys=True)
                                interval_data = float(alert_data.get('interval'))
                                callback_data = json.dumps(alert_data['callback']['__data__'], sort_keys=True)
                                deadline_alert_id = str(uuid6.uuid7())

                                conn.execute(
                                    sa.text("""
                                            INSERT INTO deadline_alert (
                                                id,
                                                created_at,
                                                dag_id,
                                                reference,
                                                interval,
                                                callback,
                                                name,
                                                description)
                                            VALUES (
                                                :id,
                                                :created_at,
                                                :dag_id,
                                                :reference,
                                                :interval,
                                                :callback,
                                                NULL,
                                                NULL)
                                            """),
                                    {
                                        "id": deadline_alert_id,
                                        "created_at": created_at or timezone.utcnow(),
                                        "dag_id": dag_id,
                                        "reference": reference_data,
                                        "interval": interval_data,
                                        "callback": callback_data,
                                    },
                                )

                                if not validate_written_data(
                                    conn, deadline_alert_id, reference_data, interval_data, callback_data
                                ):
                                    dags_with_errors.add(dag_id)
                                    continue

                                # TODO:  replace_old_data(conn, dag_id, deadline_alert_id)
                                migrated_alerts_count += 1

                                conn.execute(
                                    sa.text("""
                                            UPDATE deadline
                                            SET deadline_alert_id = :alert_id
                                            WHERE dagrun_id IN (SELECT id FROM dag_run WHERE dag_id = :dag_id)
                                              AND deadline_alert_id IS NULL
                                            """),
                                    {"alert_id": deadline_alert_id, "dag_id": dag_id},
                                )
                            except Exception as e:
                                print(f"WARNING: Failed to process alert in DAG {dag_id}: {e}")
                                dags_with_errors.add(dag_id)
                                continue

            except (json.JSONDecodeError, KeyError, TypeError) as e:
                print(f"WARNING: Could not process serialized DAG {dag_id}: {e}")
                dags_with_errors.add(dag_id)

        print(f"Batch {batch_num} of {total_batches} complete.")

    print("\nMigration complete!")
    print(f"Processed {processed_dags_count} DAGs, {dags_with_deadlines_count} had DeadlineAlerts.")
    print(f"Migrated {migrated_alerts_count} DeadlineAlert configurations.")
    if dags_with_errors:
        print(f"{len(dags_with_errors)} DAGs encountered errors: {dags_with_errors}")
    else:
        print("No DAGs encountered errors during migration.")


def validate_written_data(conn, deadline_alert_id, expected_reference, expected_interval, expected_callback):
    """Read back the inserted data and validate that is matches what we expect."""
    # TODO: Is this overkill?

    validation_result = conn.execute(
        sa.text("""
                SELECT reference, interval, callback
                FROM deadline_alert
                WHERE id = :alert_id
                """),
        {"alert_id": deadline_alert_id}
    ).fetchone()

    if not validation_result:
        print(f"ERROR: Failed to read back deadline_alert for DeadlineAlert {deadline_alert_id}")
        return False

    checks = [
        ("REFERENCE", json.dumps(validation_result.reference, sort_keys=True), expected_reference),
        ("INTERVAL", validation_result.interval, expected_interval),
        ("CALLBACK", json.dumps(validation_result.callback, sort_keys=True), expected_callback)
    ]

    for name, actual, expected in checks:
        if actual != expected:
            print(f"ERROR: Written {name} does not match expected! Written: {actual}, Expected: {expected}")
            return False

    return True


def migrate_deadline_alert_data_back_to_serialized_dag():
    # TODO: THIS
    ...
