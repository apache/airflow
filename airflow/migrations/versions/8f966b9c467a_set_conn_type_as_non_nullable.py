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

"""Set conn_type as non-nullable

Revision ID: 8f966b9c467a
Revises: 3c20cacc0044
Create Date: 2020-06-08 22:36:34.534121

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8f966b9c467a"
down_revision = "3c20cacc0044"
branch_labels = None
depends_on = None


def upgrade():
    """Apply Set conn_type as non-nullable"""

    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.alter_column("conn_type", existing_type=sa.VARCHAR(length=500), nullable=False)


def downgrade():
    """Unapply Set conn_type as non-nullable"""
    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.alter_column("conn_type", existing_type=sa.VARCHAR(length=500), nullable=True)
