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
"""Merge migrations Heads.

Revision ID: 03bc53e68815
Revises: 0a2a5b66e19d, bf00311e1990
Create Date: 2018-11-24 20:21:46.605414

"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "03bc53e68815"
down_revision = ("0a2a5b66e19d", "bf00311e1990")
branch_labels = None
depends_on = None
airflow_version = "1.10.2"


def upgrade():
    op.create_index("sm_dag", "sla_miss", ["dag_id"], unique=False)


def downgrade():
    op.drop_index("sm_dag", table_name="sla_miss")
