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

"""Add stored procedures for task selection

Revision ID: 28abf6d28d2f
Revises: 2f49f2dae90c
Create Date: 2025-09-12 23:20:39.502229

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "28abf6d28d2f"
down_revision = "2f49f2dae90c"
branch_labels = None
depends_on = None


def upgrade():
    """Apply Add stored procedures for task selection"""
    pass


def downgrade():
    """Unapply Add stored procedures for task selection"""
    pass
