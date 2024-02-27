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

"""increase Log event string size

Revision ID: e54d90c77877
Revises: 88344c1d9134
Create Date: 2024-02-26 20:35:06.589818

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = 'e54d90c77877'
down_revision = '88344c1d9134'
branch_labels = None
depends_on = None
airflow_version = '2.9.0'


def upgrade():
    """Apply increase Log event string size"""
    with op.batch_alter_table("log") as batch_op:
        batch_op.alter_column("event", type_=sa.String(60))


def downgrade():
    """Unapply increase Log event string size"""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        with op.batch_alter_table("log") as batch_op:
            batch_op.drop_index("idx_log_event")
            batch_op.alter_column("event", type_=sa.String(30), nullable=False)
            batch_op.create_index("idx_log_event", ["event"])
    else:
        with op.batch_alter_table("log") as batch_op:
            batch_op.alter_column("event", type_=sa.String(30), nullable=False)
