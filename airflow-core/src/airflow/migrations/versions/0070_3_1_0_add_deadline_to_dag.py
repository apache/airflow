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
Add Deadline to Dag.

Revision ID: dfee8bd5d574
Revises: fe199e1abd77
Create Date: 2024-12-18 19:10:26.962464
"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy_jsonfield
from alembic import op

from airflow.settings import json

revision = "dfee8bd5d574"
down_revision = "fe199e1abd77"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    op.add_column(
        "dag",
        sa.Column("deadline", sqlalchemy_jsonfield.JSONField(json=json), nullable=True),
    )


def downgrade():
    op.drop_column("dag", "deadline")
