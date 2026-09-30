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
Add lang_sdk_task_handler_artifact and lang_sdk_task_handler tables.

Revision ID: f7ed13533d23
Revises: 90e4d18ccadf
Create Date: 2026-09-30 14:19:35.109799

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "f7ed13533d23"
down_revision = "90e4d18ccadf"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add lang_sdk_task_handler_artifact and lang_sdk_task_handler tables."""
    op.create_table(
        "lang_sdk_task_handler_artifact",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("bundle_name", StringID(), nullable=False),
        sa.Column("relative_fileloc", sa.String(length=2000), nullable=False),
        sa.Column("relative_fileloc_hash", sa.String(length=32), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("cache_digest", sa.String(length=128), nullable=False),
        sa.Column("last_probed_at", UtcDateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("lang_sdk_task_handler_artifact_pkey")),
        sa.UniqueConstraint(
            "bundle_name",
            "relative_fileloc_hash",
            name=op.f("lang_sdk_task_handler_artifact_bundle_fileloc_uq"),
        ),
    )
    op.create_table(
        "lang_sdk_task_handler",
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("task_id", StringID(), nullable=False),
        sa.Column("artifact_id", sa.Uuid(), nullable=False),
        sa.Column("dag_bundle_name", StringID(), nullable=False),
        sa.Column("dag_relative_fileloc", sa.String(length=2000), nullable=False),
        sa.Column("dag_relative_fileloc_hash", sa.String(length=32), nullable=False),
        sa.Column("handler_binding", sa.String(length=20), nullable=False),
        sa.Column("handler_params", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("dag_id", "task_id", name=op.f("lang_sdk_task_handler_pkey")),
        sa.ForeignKeyConstraint(
            columns=("dag_id",),
            refcolumns=["dag.dag_id"],
            name="lang_sdk_task_handler_dag_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            columns=("artifact_id",),
            refcolumns=["lang_sdk_task_handler_artifact.id"],
            name="lang_sdk_task_handler_artifact_id_fkey",
        ),
        sa.Index("idx_lang_sdk_task_handler_dag_file", "dag_bundle_name", "dag_relative_fileloc_hash"),
        sa.Index("idx_lang_sdk_task_handler_artifact_id", "artifact_id"),
    )


def downgrade():
    """Drop lang_sdk_task_handler and lang_sdk_task_handler_artifact tables."""
    op.drop_table("lang_sdk_task_handler")
    op.drop_table("lang_sdk_task_handler_artifact")
