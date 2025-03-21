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
Rename dag_schedule_dataset_alias_reference constraint names.

Revision ID: 5f2621c13b39
Revises: 22ed7efa9da2
Create Date: 2024-10-25 04:03:33.002701

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import mysql_drop_foreignkey_if_exists
from airflow.models import ID_LEN
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "5f2621c13b39"
down_revision = "22ed7efa9da2"
branch_labels = None
depends_on = None
airflow_version = "2.10.3"


def mysql_create_foreignkey_if_not_exists(
    constraint_name, table_name, column_name, ref_table, ref_column, op
):
    op.execute(f"""
    CREATE PROCEDURE create_foreign_key_if_not_exists()
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
            SELECT 1;
        ELSE
            ALTER TABLE {table_name}
            ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column_name})
            REFERENCES {ref_table}({ref_column})
            ON DELETE CASCADE;
        END IF;
    END;
    CALL create_foreign_key_if_not_exists();
    DROP PROCEDURE create_foreign_key_if_not_exists;
    """)


def postgres_create_foreignkey_if_not_exists(
    constraint_name, table_name, column_name, ref_table, ref_column, op
):
    op.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.table_constraints
                WHERE constraint_type = 'FOREIGN KEY'
                  AND constraint_name = '{constraint_name}'
            ) THEN
                ALTER TABLE {table_name}
                ADD CONSTRAINT {constraint_name}
                FOREIGN KEY ({column_name})
                REFERENCES {ref_table} ({ref_column})
                ON DELETE CASCADE;
            END IF;
        END $$;
    """)


def upgrade():
    """Rename dag_schedule_dataset_alias_reference constraint."""
    dialect = op.get_context().dialect.name
    if dialect == "sqlite":
        op.create_table(
            "new_table",
            sa.Column("alias_id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("dag_id", sa.String(ID_LEN), primary_key=True, nullable=False),
            sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
            sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(
                ("alias_id",),
                ["dataset_alias.id"],
                name="dsdar_dataset_alias_fkey",
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                columns=("dag_id",),
                refcolumns=["dag.dag_id"],
                name="dsdar_dag_id_fkey",
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("alias_id", "dag_id", name="dsdar_pkey"),
        )
        op.execute("INSERT INTO new_table SELECT * FROM dag_schedule_dataset_alias_reference")
        op.drop_table("dag_schedule_dataset_alias_reference")
        op.rename_table("new_table", "dag_schedule_dataset_alias_reference")
        op.create_index(
            "idx_dag_schedule_dataset_alias_reference_dag_id",
            "dag_schedule_dataset_alias_reference",
            ["dag_id"],
            unique=False,
        )
    if dialect == "postgresql":
        op.execute(
            "ALTER TABLE dag_schedule_dataset_alias_reference DROP CONSTRAINT IF EXISTS dsdar_dataset_fkey"
        )
        op.execute(
            "ALTER TABLE dag_schedule_dataset_alias_reference DROP CONSTRAINT IF EXISTS dsdar_dag_fkey"
        )
        postgres_create_foreignkey_if_not_exists(
            "dsdar_dataset_alias_fkey",
            "dag_schedule_dataset_alias_reference",
            "alias_id",
            "dataset_alias",
            "id",
            op,
        )
        postgres_create_foreignkey_if_not_exists(
            "dsdar_dag_id_fkey", "dag_schedule_dataset_alias_reference", "alias_id", "dataset_alias", "id", op
        )
    if dialect == "mysql":
        mysql_drop_foreignkey_if_exists("dsdar_dataset_fkey", "dag_schedule_dataset_alias_reference", op)
        mysql_drop_foreignkey_if_exists("dsdar_dag_fkey", "dag_schedule_dataset_alias_reference", op)
        mysql_create_foreignkey_if_not_exists(
            "dsdar_dataset_alias_fkey",
            "dag_schedule_dataset_alias_reference",
            "alias_id",
            "dataset_alias",
            "id",
            op,
        )
        mysql_create_foreignkey_if_not_exists(
            "dsdar_dag_id_fkey", "dag_schedule_dataset_alias_reference", "alias_id", "dataset_alias", "id", op
        )


def downgrade():
    """Undo dag_schedule_dataset_alias_reference constraint rename."""
    dialect = op.get_context().dialect.name
    if dialect == "postgresql":
        op.execute(
            "ALTER TABLE dag_schedule_dataset_alias_reference DROP CONSTRAINT IF EXISTS dsdar_dataset_alias_fkey"
        )
        op.execute(
            "ALTER TABLE dag_schedule_dataset_alias_reference DROP CONSTRAINT IF EXISTS dsdar_dag_id_fkey"
        )
        postgres_create_foreignkey_if_not_exists(
            "dsdar_dataset_fkey",
            "dag_schedule_dataset_alias_reference",
            "alias_id",
            "dataset_alias",
            "id",
            op,
        )
        postgres_create_foreignkey_if_not_exists(
            "dsdar_dag_fkey",
            "dag_schedule_dataset_alias_reference",
            "alias_id",
            "dataset_alias",
            "id",
            op,
        )
    if dialect == "mysql":
        mysql_drop_foreignkey_if_exists(
            "dsdar_dataset_alias_fkey", "dag_schedule_dataset_alias_reference", op
        )
        mysql_drop_foreignkey_if_exists("dsdar_dag_id_fkey", "dag_schedule_dataset_alias_reference", op)
        mysql_create_foreignkey_if_not_exists(
            "dsdar_dataset_fkey",
            "dag_schedule_dataset_alias_reference",
            "alias_id",
            "dataset_alias",
            "id",
            op,
        )
        mysql_create_foreignkey_if_not_exists(
            "dsdar_dag_fkey",
            "dag_schedule_dataset_alias_reference",
            "alias_id",
            "dataset_alias",
            "id",
            op,
        )
    if dialect == "sqlite":
        op.create_table(
            "new_table",
            sa.Column("alias_id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("dag_id", sa.String(ID_LEN), primary_key=True, nullable=False),
            sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
            sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(
                ("alias_id",),
                ["dataset_alias.id"],
                name="dsdar_dataset_fkey",
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                columns=("dag_id",),
                refcolumns=["dag.dag_id"],
                name="dsdar_dag_fkey",
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("alias_id", "dag_id", name="dsdar_pkey"),
        )
        op.execute("INSERT INTO new_table SELECT * FROM dag_schedule_dataset_alias_reference")
        op.drop_table("dag_schedule_dataset_alias_reference")
        op.rename_table("new_table", "dag_schedule_dataset_alias_reference")
        op.create_index(
            "idx_dag_schedule_dataset_alias_reference_dag_id",
            "dag_schedule_dataset_alias_reference",
            ["dag_id"],
            unique=False,
        )
