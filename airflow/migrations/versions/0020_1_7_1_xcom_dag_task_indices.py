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
Add indices on ``xcom`` table.

Revision ID: 8504051e801b
Revises: 4addfa1236f1
Create Date: 2016-11-29 08:13:03.253312

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "8504051e801b"
down_revision = "4addfa1236f1"
branch_labels = None
depends_on = None
airflow_version = "1.7.1.3"


def upgrade():
    """Create Index."""
    op.create_index("idx_xcom_dag_task_date", "xcom", ["dag_id", "task_id", "execution_date"], unique=False)


def downgrade():
    """Drop Index."""
    op.drop_index("idx_xcom_dag_task_date", table_name="xcom")
