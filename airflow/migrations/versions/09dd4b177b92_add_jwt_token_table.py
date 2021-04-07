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

"""add jwt token table

Revision ID: 09dd4b177b92
Revises: 90d1635d7b86
Create Date: 2021-04-07 17:06:42.061407

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '09dd4b177b92'
down_revision = '90d1635d7b86'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Add jwt token table"""
    op.create_table(
        "jwt_token",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("jti", sa.String(50), nullable=False),
        sa.Column("is_revoked", sa.Boolean(name="is_revoked"), server_default="0"),
        sa.Column("refresh", sa.Boolean(name="refresh"), server_default="0"),
        sa.Column("revoke_reason", sa.String(100)),
        sa.Column("revoked_by", sa.String(50)),
        sa.Column("date_revoked", sa.DateTime),
        sa.Column("expiry_delta", sa.Integer, nullable=False),
        sa.Column("created_delta", sa.Integer, nullable=False),
    )


def downgrade():  # noqa: D103
    """Unapply Add jwt token table"""
    op.drop_table('jwt_token')
