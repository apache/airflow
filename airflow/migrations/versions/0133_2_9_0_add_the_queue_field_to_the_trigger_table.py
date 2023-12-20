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

"""Add the queue field to the trigger table.

Revision ID: e8b1fd3c7ccf
Revises: 10b52ebd31f7
Create Date: 2023-12-20 21:06:00.768844

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = 'e8b1fd3c7ccf'
down_revision = '10b52ebd31f7'
branch_labels = None
depends_on = None
airflow_version = "2.9.0"


def upgrade():
    """Apply Add the queue field to the trigger table."""
    with op.batch_alter_table("trigger") as batch_op:
        try:
            from airflow.configuration import conf

            default_queue = conf.get("triggerer", "default_queue")
        except:  # noqa
            default_queue = "default"

        batch_op.add_column(
            sa.Column(
                "queue",
                sa.String(256),
                nullable=True,
                server_default=default_queue,
            )
        )
        batch_op.alter_column("queue", server_default=None)


def downgrade():
    """Unapply Add the queue field to the trigger table."""
    with op.batch_alter_table("trigger") as batch_op:
        batch_op.drop_column("queue", mssql_drop_default=True)
