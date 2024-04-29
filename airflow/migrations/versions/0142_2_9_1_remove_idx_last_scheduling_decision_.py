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

import sqlalchemy as sa
from alembic import op

from contextlib import suppress

# revision identifiers, used by Alembic.
revision = 'bff083ad727d'
down_revision = '677fdbb7fc54'
branch_labels = None
depends_on = None
airflow_version = "2.9.1"


def upgrade():
    """Apply Remove idx_last_scheduling_decision index on last_scheduling_decision in dag_run table"""
    try:
        op.drop_index("idx_last_scheduling_decision", table_name="dag_run", if_exists=True)
    except sa.exc.DatabaseError:
        # MySQL does not support drop if exists index. Try to drop the index directly instead.
        with suppress(sa.exc.DatabaseError):
            # Suppress the error if the index does not exist.
            op.drop_index("idx_last_scheduling_decision", table_name="dag_run")


def downgrade():
    """Unapply Remove idx_last_scheduling_decision index on last_scheduling_decision in dag_run table"""

    try:
        op.create_index("idx_last_scheduling_decision", "dag_run", ["last_scheduling_decision"],
                        unique=False, if_not_exists=True)
    except sa.exc.DatabaseError:
        # MySQL does not support create if not exists index. Try to create the index directly instead.
        with suppress(sa.exc.DatabaseError):
            # Suppress the error if the index already exists.
            op.create_index("idx_last_scheduling_decision", "dag_run", ["last_scheduling_decision"], unique=False)
