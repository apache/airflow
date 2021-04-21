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

"""add-token-blocklist

Revision ID: 22ab4efd5674
Revises: a13f7613ad25
Create Date: 2021-04-17 18:16:31.019394

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '22ab4efd5674'
down_revision = 'a13f7613ad25'
branch_labels = None
depends_on = None

INDEX_NAME = "idx_expiry_date_token_blocklist"
TABLE_NAME = "token_blocklist"


def upgrade():
    """Apply Add token blocklist table"""
    op.create_table(
        TABLE_NAME,
        sa.Column("jti", sa.String(50), nullable=False, primary_key=True),
        sa.Column("expiry_date", sa.DateTime(), nullable=False),
    )
    op.create_index(INDEX_NAME, TABLE_NAME, ['expiry_date'], unique=False)


def downgrade():  # noqa: D103
    """Unapply Add token blocklist table"""
    op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
    op.drop_table(TABLE_NAME)
