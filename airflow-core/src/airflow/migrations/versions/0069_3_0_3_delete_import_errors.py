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
Delete import errors.

Revision ID: fe199e1abd77
Revises: 29ce7909c52b
Create Date: 2025-06-10 08:53:28.782896

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fe199e1abd77"
down_revision = "29ce7909c52b"
branch_labels = None
depends_on = None

airflow_version = "3.0.3"


def upgrade():
    """Apply Delete import errors."""
    # delete import_error table rows
    op.get_bind().execute(sa.text("DELETE FROM import_error"))


def downgrade():
    """Unapply Delete import errors."""
    pass
