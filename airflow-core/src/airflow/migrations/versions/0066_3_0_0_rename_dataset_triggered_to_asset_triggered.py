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
Rename run_type from 'dataset_triggered' to 'asset_triggered' in dag_run table.

Revision ID: 0e9519b56710
Revises: ec62e120484d
Create Date: 2025-04-05 11:15:24.526167

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0e9519b56710"
down_revision = "ec62e120484d"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply rename run_type from 'dataset_triggered' to 'asset_triggered' in dag_run table."""
    op.execute("update dag_run set run_type = 'asset_triggered' where run_type = 'dataset_triggered'")


def downgrade():
    """Unapply rename run_type from 'asset_triggered' to 'dataset_triggered' in dag_run table."""
    op.execute("update dag_run set run_type = 'dataset_triggered' where run_type = 'asset_triggered'")
