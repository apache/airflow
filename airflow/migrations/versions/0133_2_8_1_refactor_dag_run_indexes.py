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

"""Drop unused TI index

Revision ID: 88344c1d9134
Revises: 10b52ebd31f7
Create Date: 2024-01-11 11:54:48.232030

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "88344c1d9134"
down_revision = "10b52ebd31f7"
branch_labels = None
depends_on = None
airflow_version = "2.8.1"


def upgrade():
    """Apply refactor dag run indexes"""
    # This index may have been created in 2.7 but we've since removed it from migrations
    import sqlalchemy
    from contextlib import suppress

    with suppress(sqlalchemy.exc.DatabaseError):  # mysql does not support drop if exists index
        op.drop_index("ti_state_incl_start_date", table_name="task_instance", if_exists=True)


def downgrade():
    """Unapply refactor dag run indexes"""
