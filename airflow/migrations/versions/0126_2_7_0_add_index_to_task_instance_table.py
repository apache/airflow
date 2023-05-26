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

"""Add index to task_instance table

Revision ID: 937cbd173ca1
Revises: c804e5c76e3e
Create Date: 2023-05-03 11:31:32.527362

"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "937cbd173ca1"
down_revision = "c804e5c76e3e"
branch_labels = None
depends_on = None
airflow_version = "2.7.0"


def upgrade():
    """Apply Add index to task_instance table"""
    op.create_index(
        "ti_state_incl_start_date",
        "task_instance",
        ["dag_id", "task_id", "state"],
        postgresql_include=["start_date"],
    )


def downgrade():
    """Unapply Add index to task_instance table"""
    op.drop_index("ti_state_incl_start_date", table_name="task_instance")
