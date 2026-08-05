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
"""
Add index on asset.uri.

Revision ID: c4e7a1f9b2d0
Revises: 436dc127462c
Create Date: 2026-07-06 00:00:00.000000
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "c4e7a1f9b2d0"
down_revision = "436dc127462c"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Apply Add index on asset.uri."""
    with op.batch_alter_table("asset", schema=None) as batch_op:
        batch_op.create_index("idx_asset_uri", ["uri"], unique=False)


def downgrade():
    """Unapply Add index on asset.uri."""
    with op.batch_alter_table("asset", schema=None) as batch_op:
        batch_op.drop_index("idx_asset_uri")
