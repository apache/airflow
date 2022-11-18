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
"""Add ``creating_job_id`` to ``DagRun`` table

Revision ID: 364159666cbd
Revises: 52d53670a240
Create Date: 2020-10-10 09:08:07.332456

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "364159666cbd"
down_revision = "52d53670a240"
branch_labels = None
depends_on = None
airflow_version = "2.0.0"


def upgrade():
    """Apply Add ``creating_job_id`` to ``DagRun`` table"""
    op.add_column("dag_run", sa.Column("creating_job_id", sa.Integer))


def downgrade():
    """Unapply Add job_id to DagRun table"""
    op.drop_column("dag_run", "creating_job_id")
