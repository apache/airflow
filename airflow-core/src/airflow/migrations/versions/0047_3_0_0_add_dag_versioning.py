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
add dag versioning.

Revision ID: 2b47dc6bc8df
Revises: d03e4a635aa3
Create Date: 2024-10-09 05:44:04.670984

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils import UUIDType

from airflow.migrations.db_types import TIMESTAMP, StringID
from airflow.models.base import naming_convention
from airflow.utils import timezone

# revision identifiers, used by Alembic.
revision = "2b47dc6bc8df"
down_revision = "d03e4a635aa3"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply add dag versioning."""
    op.execute("delete from dag_code;")
    op.execute("delete from serialized_dag;")

    op.create_table(
        "dag_version",
        sa.Column("id", UUIDType(binary=False), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("created_at", TIMESTAMP(), nullable=False, default=timezone.utcnow),
        sa.Column(
            "last_updated", TIMESTAMP(), nullable=False, default=timezone.utcnow, onupdate=timezone.utcnow
        ),
        sa.ForeignKeyConstraint(
            ("dag_id",), ["dag.dag_id"], name=op.f("dag_version_dag_id_fkey"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("dag_version_pkey")),
        sa.UniqueConstraint("dag_id", "version_number", name="dag_id_v_name_v_number_unique_constraint"),
    )
    with op.batch_alter_table("dag_code") as batch_op:
        batch_op.drop_constraint("dag_code_pkey", type_="primary")
        batch_op.drop_column("fileloc_hash")
        batch_op.add_column(sa.Column("id", UUIDType(binary=False), nullable=False))
        batch_op.create_primary_key("dag_code_pkey", ["id"])
        batch_op.add_column(sa.Column("dag_version_id", UUIDType(binary=False), nullable=False))
        batch_op.add_column(sa.Column("source_code_hash", sa.String(length=32), nullable=False))
        batch_op.add_column(sa.Column("dag_id", StringID(), nullable=False))
        batch_op.add_column(sa.Column("created_at", TIMESTAMP(), default=timezone.utcnow, nullable=False))
        batch_op.create_foreign_key(
            batch_op.f("dag_code_dag_version_id_fkey"),
            "dag_version",
            ["dag_version_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_unique_constraint("dag_code_dag_version_id_uq", ["dag_version_id"])

    with op.batch_alter_table("serialized_dag") as batch_op:
        batch_op.drop_constraint("serialized_dag_pkey", type_="primary")
        batch_op.drop_index("idx_fileloc_hash")
        batch_op.drop_column("fileloc_hash")
        batch_op.drop_column("fileloc")
        batch_op.add_column(sa.Column("id", UUIDType(binary=False), nullable=False))
        batch_op.add_column(sa.Column("dag_version_id", UUIDType(binary=False), nullable=False))
        batch_op.add_column(sa.Column("created_at", TIMESTAMP(), default=timezone.utcnow, nullable=False))
        batch_op.create_primary_key("serialized_dag_pkey", ["id"])
        batch_op.create_foreign_key(
            batch_op.f("serialized_dag_dag_version_id_fkey"),
            "dag_version",
            ["dag_version_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_unique_constraint("serialized_dag_dag_version_id_uq", ["dag_version_id"])

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.add_column(sa.Column("dag_version_id", UUIDType(binary=False)))
        batch_op.create_foreign_key(
            batch_op.f("task_instance_dag_version_id_fkey"),
            "dag_version",
            ["dag_version_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(sa.Column("dag_version_id", UUIDType(binary=False)))

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("dag_hash")
        batch_op.add_column(sa.Column("created_dag_version_id", UUIDType(binary=False), nullable=True))
        batch_op.create_foreign_key(
            "created_dag_version_id_fkey",
            "dag_version",
            ["created_dag_version_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade():
    """Unapply add dag versioning."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("task_instance_dag_version_id_fkey"), type_="foreignkey")
        batch_op.drop_column("dag_version_id")

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.drop_column("dag_version_id")

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("dag_hash", sa.String(length=32), autoincrement=False, nullable=True))
        batch_op.drop_constraint("created_dag_version_id_fkey", type_="foreignkey")
        batch_op.drop_column("created_dag_version_id")

    op.execute("delete from dag_code;")
    op.execute("delete from serialized_dag;")

    with op.batch_alter_table("dag_code", schema=None) as batch_op:
        batch_op.drop_constraint("dag_code_pkey", type_="primary")
        batch_op.drop_constraint(batch_op.f("dag_code_dag_version_id_fkey"), type_="foreignkey")
        batch_op.add_column(sa.Column("fileloc_hash", sa.BigInteger, nullable=True))
        batch_op.create_primary_key("dag_code_pkey", ["fileloc_hash"])
        batch_op.drop_column("source_code_hash")
        batch_op.drop_column("created_at")
        batch_op.drop_column("id")
        batch_op.drop_column("dag_version_id")
        batch_op.drop_column("dag_id")

    with op.batch_alter_table("serialized_dag", schema=None, naming_convention=naming_convention) as batch_op:
        batch_op.drop_column("id")
        batch_op.drop_constraint(batch_op.f("serialized_dag_dag_version_id_fkey"), type_="foreignkey")
        batch_op.drop_column("created_at")
        batch_op.drop_column("dag_version_id")
        batch_op.add_column(sa.Column("fileloc", sa.String(length=2000), nullable=False))
        batch_op.add_column(sa.Column("fileloc_hash", sa.BIGINT(), nullable=False))
        batch_op.create_index("idx_fileloc_hash", ["fileloc_hash"], unique=False)
        batch_op.create_primary_key("serialized_dag_pkey", ["dag_id"])

    op.drop_table("dag_version")
