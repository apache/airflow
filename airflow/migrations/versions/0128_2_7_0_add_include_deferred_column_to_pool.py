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

"""add include_deferred column to pool

Revision ID: 405de8318b3a
Revises: 788397e78828
Create Date: 2023-07-20 04:22:21.007342

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "405de8318b3a"
down_revision = "788397e78828"
branch_labels = None
depends_on = None
airflow_version = "2.7.0"


def upgrade():
    """Apply add include_deferred column to pool"""
    with op.batch_alter_table("slot_pool") as batch_op:
        batch_op.add_column(sa.Column("include_deferred", sa.Boolean))
    # Different databases support different literal for FALSE. This is fine.
    op.execute(sa.text(f"UPDATE slot_pool SET include_deferred = {sa.false().compile(op.get_bind())}"))
    with op.batch_alter_table("slot_pool") as batch_op:
        batch_op.alter_column("include_deferred", existing_type=sa.Boolean, nullable=False)


def downgrade():
    """Unapply add include_deferred column to pool"""
    with op.batch_alter_table("slot_pool") as batch_op:
        batch_op.drop_column("include_deferred")
