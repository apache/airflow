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
Add Human In the Loop Detail History table.

Revision ID: 5cc8117e9285
Revises: 1b2c3d4e5f6g
Create Date: 2025-09-22 13:21:21.957132

"""

from __future__ import annotations

import sqlalchemy_jsonfield
from alembic import op
from sqlalchemy import Boolean, Column, ForeignKeyConstraint, String, Text
from sqlalchemy.dialects import postgresql

from airflow._shared.timezones import timezone
from airflow.settings import json
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "5cc8117e9285"
down_revision = "1b2c3d4e5f6g"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add Human In the Loop Detail History table."""
    op.create_table(
        "hitl_detail_history",
        Column(
            "ti_history_id",
            String(length=36).with_variant(postgresql.UUID(), "postgresql"),
            primary_key=True,
            nullable=False,
        ),
        Column("options", sqlalchemy_jsonfield.JSONField(json=json), nullable=False),
        Column("subject", Text, nullable=False),
        Column("body", Text, nullable=True),
        Column("defaults", sqlalchemy_jsonfield.JSONField(json=json), nullable=True),
        Column("multiple", Boolean, unique=False, default=False),
        Column("params", sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={}),
        Column("assignees", sqlalchemy_jsonfield.JSONField(json=json), nullable=True),
        Column("created_at", UtcDateTime(timezone=True), nullable=False, default=timezone.utcnow),
        Column("responded_at", UtcDateTime, nullable=True),
        Column("responded_by", sqlalchemy_jsonfield.JSONField(json=json), nullable=True),
        Column("chosen_options", sqlalchemy_jsonfield.JSONField(json=json), nullable=True),
        Column("params_input", sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={}),
        ForeignKeyConstraint(
            ["ti_history_id"],
            ["task_instance_history.task_instance_id"],
            name="hitl_detail_history_tih_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )


def downgrade():
    """Response Human In the Loop Detail History table."""
    op.drop_table("hitl_detail_history")
