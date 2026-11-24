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
Add tables for Dag bundle manifests stored in the metadata database.

Revision ID: 0541ebdbf0e7
Revises: c64b10719069
Create Date: 2026-11-16 22:35:31.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.models.base import StringID
from airflow.utils.sqlalchemy import UtcDateTime

revision = "0541ebdbf0e7"
down_revision = "c64b10719069"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add the manifest and release-reference tables for database-backed Dag bundle artifacts."""
    op.create_table(
        "dag_bundle_manifest",
        sa.Column("bundle_name", StringID(length=250), nullable=False),
        sa.Column("bundle_version", StringID(length=200), nullable=False),
        sa.Column("manifest", sa.JSON(), nullable=False),
        sa.Column("created_at", UtcDateTime(), nullable=False),
        sa.PrimaryKeyConstraint("bundle_name", "bundle_version", name=op.f("dag_bundle_manifest_pkey")),
    )
    op.create_table(
        "dag_bundle_manifest_ref",
        sa.Column("bundle_name", StringID(length=250), nullable=False),
        sa.Column("version", StringID(length=200), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("updated_at", UtcDateTime(), nullable=False),
        sa.PrimaryKeyConstraint("bundle_name", name=op.f("dag_bundle_manifest_ref_pkey")),
    )


def downgrade():
    """Remove the database-backed Dag bundle artifact tables."""
    op.drop_table("dag_bundle_manifest_ref")
    op.drop_table("dag_bundle_manifest")
