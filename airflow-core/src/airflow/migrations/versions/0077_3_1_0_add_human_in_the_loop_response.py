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
Add Human In the Loop Response table.

Revision ID: 40f7c30a228b
Revises: 5d3072c51bac
Create Date: 2025-07-04 15:05:19.459197

"""

from __future__ import annotations

import sqlalchemy_jsonfield
from alembic import op
from sqlalchemy import Boolean, Column, String, Text
from sqlalchemy.dialects import postgresql

from airflow.settings import json
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "40f7c30a228b"
down_revision = "5d3072c51bac"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Add Human In the Loop Response table."""
    op.create_table(
        "hitl_response",
        Column(
            "ti_id",
            String(length=36).with_variant(postgresql.UUID(), "postgresql"),
            primary_key=True,
            nullable=False,
        ),
        Column("options", sqlalchemy_jsonfield.JSONField(json=json), nullable=False),
        Column("subject", Text, nullable=False),
        Column("body", Text, nullable=True),
        Column("default", sqlalchemy_jsonfield.JSONField(json=json), nullable=True),
        Column("multiple", Boolean, unique=False, default=False),
        Column("params", sqlalchemy_jsonfield.JSONField(json=json), nullable=True),
        Column("response_at", UtcDateTime, nullable=True),
        Column("user_id", String(128), nullable=True),
        Column("response_content", sqlalchemy_jsonfield.JSONField(json=json), nullable=True),
        Column("params_input", sqlalchemy_jsonfield.JSONField(json=json), nullable=True, default=None),
    )


def downgrade():
    """Response Human In the Loop Response table."""
    op.drop_table("hitl_response")
