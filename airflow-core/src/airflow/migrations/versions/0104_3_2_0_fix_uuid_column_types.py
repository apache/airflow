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
Standardize UUID column format for non-PostgreSQL databases.

Several columns used String(36) to store UUIDs in the 36-character dashed format
(e.g., "019c32fd-f36f-77a4-b5ba-098064c38b15"). This migration standardizes UUID
storage to the 32-character hex format (e.g., "019c32fdf36f77a4b5ba098064c38b15")
used by SQLAlchemy's native sa.Uuid() type on non-PostgreSQL databases.

Affected columns:
  - task_instance.id (PK)
  - task_instance_note.ti_id (FK -> task_instance.id)
  - task_reschedule.ti_id (FK -> task_instance.id)
  - hitl_detail.ti_id (FK -> task_instance.id)
  - task_instance_history.task_instance_id (PK)
  - hitl_detail_history.ti_history_id (FK -> task_instance_history.task_instance_id)

Revision ID: f8c9d7e6b5a4
Revises: 53ff648b8a26
Create Date: 2026-02-06 12:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f8c9d7e6b5a4"
down_revision = "53ff648b8a26"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """
    Standardize UUID storage format for non-PostgreSQL databases.

    On PostgreSQL: No action needed (uses native UUID type).
    On MySQL/SQLite: Convert UUID values from 36-char dashed format to 32-char hex format
    and change column type from VARCHAR(36) to CHAR(32) to match sa.Uuid() behavior.
    """
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    # PostgreSQL uses native UUID type, no fix needed
    if dialect_name == "postgresql":
        return

    # Step 1: Drop all incoming FK constraints that reference the columns being changed.
    # Even with ON UPDATE CASCADE, we need to drop FKs because we're changing the column
    # TYPE (not just data). On SQLite, batch_alter_table recreates tables entirely, so
    # incoming FK references would point to the old (dropped) table.

    # FKs referencing task_instance.id
    with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
        batch_op.drop_constraint("task_instance_note_ti_fkey", type_="foreignkey")

    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")

    with op.batch_alter_table("hitl_detail", schema=None) as batch_op:
        batch_op.drop_constraint("hitl_detail_ti_fkey", type_="foreignkey")

    # FK referencing task_instance_history.task_instance_id
    with op.batch_alter_table("hitl_detail_history", schema=None) as batch_op:
        batch_op.drop_constraint("hitl_detail_history_tih_fkey", type_="foreignkey")

    # Step 2: Convert UUID values from 36-char (with dashes) to 32-char (hex only)
    # This must happen BEFORE changing the column type to avoid truncation.
    for table, column in [
        ("task_instance", "id"),
        ("task_instance_note", "ti_id"),
        ("task_reschedule", "ti_id"),
        ("hitl_detail", "ti_id"),
        ("task_instance_history", "task_instance_id"),
        ("hitl_detail_history", "ti_history_id"),
    ]:
        conn.execute(sa.text(f"UPDATE {table} SET {column} = REPLACE({column}, '-', '')"))

    # Step 3: Change column types from String(36) to Uuid()
    # Using sa.Uuid() directly so Alembic renders the correct dialect-specific type
    # (CHAR(32) on SQLite/MySQL) and schema comparison matches the ORM models.
    for table, column in [
        ("task_instance", "id"),
        ("task_instance_note", "ti_id"),
        ("task_reschedule", "ti_id"),
        ("hitl_detail", "ti_id"),
        ("task_instance_history", "task_instance_id"),
        ("hitl_detail_history", "ti_history_id"),
    ]:
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.alter_column(
                column,
                existing_type=sa.String(36),
                type_=sa.Uuid(),
                existing_nullable=False,
            )

    # Step 4: Recreate all FK constraints
    with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "task_instance_note_ti_fkey",
            "task_instance",
            ["ti_id"],
            ["id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )

    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "task_reschedule_ti_fkey",
            "task_instance",
            ["ti_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("hitl_detail", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "hitl_detail_ti_fkey",
            "task_instance",
            ["ti_id"],
            ["id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )

    with op.batch_alter_table("hitl_detail_history", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "hitl_detail_history_tih_fkey",
            "task_instance_history",
            ["ti_history_id"],
            ["task_instance_id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )


def downgrade():
    """
    Revert UUID storage format standardization.

    On PostgreSQL: No action needed (uses native UUID type).
    On MySQL/SQLite: Revert CHAR(32) back to VARCHAR(36) and convert UUID values
    from 32-char hex format to 36-char dashed format.
    """
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    # PostgreSQL uses native UUID type, no fix needed
    if dialect_name == "postgresql":
        return

    # Step 1: Drop all FK constraints before modifying columns
    with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
        batch_op.drop_constraint("task_instance_note_ti_fkey", type_="foreignkey")

    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")

    with op.batch_alter_table("hitl_detail", schema=None) as batch_op:
        batch_op.drop_constraint("hitl_detail_ti_fkey", type_="foreignkey")

    with op.batch_alter_table("hitl_detail_history", schema=None) as batch_op:
        batch_op.drop_constraint("hitl_detail_history_tih_fkey", type_="foreignkey")

    # Step 2: Expand column types back to String(36) to make room for dashes
    for table, column in [
        ("task_instance", "id"),
        ("task_instance_note", "ti_id"),
        ("task_reschedule", "ti_id"),
        ("hitl_detail", "ti_id"),
        ("task_instance_history", "task_instance_id"),
        ("hitl_detail_history", "ti_history_id"),
    ]:
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.alter_column(
                column,
                existing_type=sa.Uuid(),
                type_=sa.String(36),
                existing_nullable=False,
            )

    # Step 3: Convert UUID values from 32-char (hex only) to 36-char (with dashes)
    # Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (8-4-4-4-12)
    for table, column in [
        ("task_instance", "id"),
        ("task_instance_note", "ti_id"),
        ("task_reschedule", "ti_id"),
        ("hitl_detail", "ti_id"),
        ("task_instance_history", "task_instance_id"),
        ("hitl_detail_history", "ti_history_id"),
    ]:
        if dialect_name == "mysql":
            conn.execute(
                sa.text(
                    f"UPDATE {table} SET {column} = CONCAT("
                    f"SUBSTR({column}, 1, 8), '-', "
                    f"SUBSTR({column}, 9, 4), '-', "
                    f"SUBSTR({column}, 13, 4), '-', "
                    f"SUBSTR({column}, 17, 4), '-', "
                    f"SUBSTR({column}, 21, 12))"
                )
            )
        else:  # sqlite
            conn.execute(
                sa.text(
                    f"UPDATE {table} SET {column} = "
                    f"SUBSTR({column}, 1, 8) || '-' || "
                    f"SUBSTR({column}, 9, 4) || '-' || "
                    f"SUBSTR({column}, 13, 4) || '-' || "
                    f"SUBSTR({column}, 17, 4) || '-' || "
                    f"SUBSTR({column}, 21, 12)"
                )
            )

    # Step 4: Recreate all FK constraints
    with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "task_instance_note_ti_fkey",
            "task_instance",
            ["ti_id"],
            ["id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )

    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "task_reschedule_ti_fkey",
            "task_instance",
            ["ti_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("hitl_detail", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "hitl_detail_ti_fkey",
            "task_instance",
            ["ti_id"],
            ["id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )

    with op.batch_alter_table("hitl_detail_history", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "hitl_detail_history_tih_fkey",
            "task_instance_history",
            ["ti_history_id"],
            ["task_instance_id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )
