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

"""add dttm index on log table

Revision ID: 6abdffdd4815
Revises: 290244fb8b83
Create Date: 2023-01-13 13:57:14.412028

"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "6abdffdd4815"
down_revision = "290244fb8b83"
branch_labels = None
depends_on = None
airflow_version = "2.6.0"


def upgrade():
    """Apply add dttm index on log table"""
    op.create_index("idx_log_dttm", "log", ["dttm"], unique=False)


def downgrade():
    """Unapply add dttm index on log table"""
    op.drop_index("idx_log_dttm", table_name="log")
