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
Add index on dag_version_id to task_instance.

Revision ID: a4c2fd67d16b
Revises: 134de42d3cb0
Create Date: 2025-03-01 00:00:00.000000

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "a4c2fd67d16b"
down_revision = "134de42d3cb0"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add index on dag_version_id to task_instance table."""
    op.create_index("ti_dag_version_id", "task_instance", ["dag_version_id"], unique=False)


def downgrade():
    """Remove index on dag_version_id from task_instance table."""
    op.drop_index("ti_dag_version_id", table_name="task_instance")
