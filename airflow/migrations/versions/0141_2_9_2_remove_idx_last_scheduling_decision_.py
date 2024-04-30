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

"""Remove ``idx_last_scheduling_decision`` index on last_scheduling_decision in dag_run table

Revision ID: bff083ad727d
Revises: 677fdbb7fc54
Create Date: 2024-04-26 12:58:00.594762

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "bff083ad727d"
down_revision = "1949afb29106"
branch_labels = None
depends_on = None
airflow_version = "2.9.2"


def upgrade():
    """Apply Remove idx_last_scheduling_decision index on last_scheduling_decision in dag_run table"""
    op.drop_index("idx_last_scheduling_decision", table_name="dag_run")


def downgrade():
    """Unapply Remove idx_last_scheduling_decision index on last_scheduling_decision in dag_run table"""
    op.create_index("idx_last_scheduling_decision", "dag_run", ["last_scheduling_decision"],
                    unique=False)
