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

"""Make connection login/password TEXT

Revision ID: bd5dfbe21f88
Revises: f7bf2a57d0a6
Create Date: 2023-09-14 17:16:24.942390

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "bd5dfbe21f88"
down_revision = "f7bf2a57d0a6"
branch_labels = None
depends_on = None
airflow_version = "2.8.0"


def upgrade():
    """Apply Make connection login/password TEXT"""
    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.alter_column(
            "login", existing_type=sa.VARCHAR(length=500), type_=sa.Text(), existing_nullable=True
        )
        batch_op.alter_column(
            "password", existing_type=sa.VARCHAR(length=5000), type_=sa.Text(), existing_nullable=True
        )


def downgrade():
    """Unapply Make connection login/password TEXT"""
    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.alter_column(
            "password", existing_type=sa.Text(), type_=sa.VARCHAR(length=5000), existing_nullable=True
        )
        batch_op.alter_column(
            "login", existing_type=sa.Text(), type_=sa.VARCHAR(length=500), existing_nullable=True
        )
