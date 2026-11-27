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
Add Dag processor callback version pin and retry time.

Revision ID: eeecfff088c4
Revises: feb8b1e719ee
Create Date: 2026-11-26 15:25:44.338999

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import sqlite_rebuilds
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "eeecfff088c4"
down_revision = "feb8b1e719ee"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Store whether a Dag processor callback is version-pinned, and when to retry decoding it."""
    with op.batch_alter_table("callback") as batch:
        batch.add_column(sa.Column("is_version_pinned", sa.Boolean(), nullable=True))
        batch.add_column(sa.Column("decode_retry_after", UtcDateTime(), nullable=True))
        batch.create_index("idx_callback_bundle_version_pinned", ["bundle_name", "is_version_pinned"])


def downgrade():
    """Remove the callback version pin, its routing index and the decode retry time."""
    with sqlite_rebuilds(op), op.batch_alter_table("callback") as batch:
        batch.drop_index("idx_callback_bundle_version_pinned")
        batch.drop_column("decode_retry_after")
        batch.drop_column("is_version_pinned")
