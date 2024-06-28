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
Add index to ``task_instance`` table.

Revision ID: bf00311e1990
Revises: dd25f486b8ea
Create Date: 2018-09-12 09:53:52.007433

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "bf00311e1990"
down_revision = "dd25f486b8ea"
branch_labels = None
depends_on = None
airflow_version = "1.10.2"


def upgrade():
    op.create_index("ti_dag_date", "task_instance", ["dag_id", "execution_date"], unique=False)


def downgrade():
    op.drop_index("ti_dag_date", table_name="task_instance")
