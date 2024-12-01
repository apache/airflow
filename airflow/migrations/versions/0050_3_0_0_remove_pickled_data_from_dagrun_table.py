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
remove pickled data from dagrun table.

Revision ID: e39a26ac59f6
Revises: eed27faa34e3
Create Date: 2024-12-01 08:33:15.425141

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy_utils import UUIDType

# revision identifiers, used by Alembic.
revision = "e39a26ac59f6"
down_revision = "eed27faa34e3"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply remove pickled data from dagrun table."""
    # Summary of the change:
    # 1. Create an archived table (`_dag_run_archive`) to store the current "pickled" data in the dag_run table
    # 2. Extract and archive the pickled data using the condition
    # 3. Delete the pickled data from the dag_run table so that we can update the column type
    # 4. Update the dag_run.conf column type to JSON from bytea

    conn = op.get_bind()
    dialect = conn.dialect.name

    # Create an archived table to store the current data
    # Create the dag_run table
    op.create_table(
        "_dag_run_archive",
        sa.Column("id", sa.Integer(), nullable=False, primary_key=True, autoincrement=True),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("queued_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("logical_date", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("start_date", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("end_date", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("state", sa.String(length=50), nullable=True),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("creating_job_id", sa.Integer(), nullable=True),
        sa.Column("external_trigger", sa.Boolean(), nullable=True),
        sa.Column("run_type", sa.String(length=50), nullable=False),
        sa.Column("triggered_by", sa.String(length=50), nullable=True),
        sa.Column("conf", sa.LargeBinary().with_variant(LONGBLOB, "mysql"), nullable=True),
        sa.Column("data_interval_start", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("data_interval_end", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_scheduling_decision", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("log_template_id", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("clear_number", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("backfill_id", sa.Integer(), nullable=True),
        sa.Column("dag_version_id", UUIDType(binary=False), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dag_id", "run_id"),
        if_not_exists=True,
    )

    # Condition to detect pickled data for different databases
    condition_templates = {
        "postgresql": "get_byte(conf, 0) = 128",
        "mysql": "HEX(SUBSTRING(conf, 1, 1)) = '80'",
        "sqlite": "substr(conf, 1, 1) = char(128)",
    }

    condition = condition_templates.get(dialect)
    if not condition:
        raise RuntimeError(f"Unsupported dialect: {dialect}")

    # Archive pickled data using the condition
    conn.execute(
        text(
            f"""
             INSERT INTO _dag_run_archive (dag_id, run_id, queued_at, logical_date, start_date, end_date, state, creating_job_id, external_trigger, run_type, triggered_by, conf, data_interval_start, data_interval_end, last_scheduling_decision, log_template_id, updated_at, clear_number, backfill_id, dag_version_id)
    SELECT dag_id, run_id, queued_at, logical_date, start_date, end_date, state, creating_job_id, external_trigger, run_type, triggered_by, conf, data_interval_start, data_interval_end, last_scheduling_decision, log_template_id, updated_at, clear_number, backfill_id, dag_version_id
    FROM  dag_run
    WHERE conf IS NOT NULL AND {condition};

                """
        )
    )

    # Delete the pickled data from the dag_run table so that we can update the column type
    conn.execute(text(f"DELETE FROM dag_run WHERE conf IS NOT NULL AND {condition}"))

    # Update the conf column type to JSON
    if dialect == "postgresql":
        op.execute(
            """
            ALTER TABLE dag_run
            ALTER COLUMN conf TYPE JSONB
            USING CASE
                WHEN conf IS NOT NULL THEN CAST(CONVERT_FROM(conf, 'UTF8') AS JSONB)
                ELSE NULL
            END
            """
        )
    elif dialect == "mysql":
        op.add_column("dag_run", sa.Column("conf_json", sa.JSON(), nullable=True))
        op.execute("UPDATE dag_run SET conf_json = CAST(conf AS CHAR CHARACTER SET utf8mb4)")
        op.drop_column("dag_run", "conf")
        op.alter_column("dag_run", "conf_json", existing_type=sa.JSON(), new_column_name="conf")
    elif dialect == "sqlite":
        # Rename the existing `conf` column to `conf_old`
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.alter_column("conf", new_column_name="conf_old")

        # Add the new `conf` column with JSON type
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.add_column(sa.Column("conf", sa.JSON(), nullable=True))

        # Migrate data from `conf_old` to `conf`
        conn.execute(
            text(
                """
                UPDATE dag_run
                SET conf = json(CAST(conf_old AS TEXT))
                WHERE conf_old IS NOT NULL
                """
            )
        )

        # Drop the old `conf_old` column
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.drop_column("conf_old")


def downgrade():
    """Unapply Remove pickled data from dagrun table."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    # Revert the conf column back to LargeBinary
    if dialect == "postgresql":
        op.execute(
            """
            ALTER TABLE dag_run
            ALTER COLUMN conf TYPE BYTEA
            USING CASE
                WHEN conf IS NOT NULL THEN CONVERT_TO(conf::TEXT, 'UTF8')
                ELSE NULL
            END
            """
        )
    elif dialect == "mysql":
        op.add_column("dag_run", sa.Column("conf_blob", LONGBLOB, nullable=True))
        op.execute("UPDATE dag_run SET conf_blob = CAST(conf AS BINARY);")
        op.drop_column("dag_run", "conf")
        op.alter_column("dag_run", "conf_blob", existing_type=LONGBLOB, new_column_name="conf")

    elif dialect == "sqlite":
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.alter_column("conf", new_column_name="conf_old")

        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.add_column(sa.Column("conf", sa.LargeBinary, nullable=True))

        conn.execute(
            text(
                """
                UPDATE dag_run
                SET conf = CAST(conf_old AS BLOB)
                WHERE conf IS NOT NULL
                """
            )
        )

        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.drop_column("conf_old")
