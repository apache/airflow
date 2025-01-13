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
add bundle_name to ParseImportError.

Revision ID: 03de77aaa4ec
Revises: e39a26ac59f6
Create Date: 2025-01-08 10:38:02.108760

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "03de77aaa4ec"
down_revision = "e39a26ac59f6"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply add bundle_name to ParseImportError."""
    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.add_column(sa.Column("bundle_name", sa.String(length=250), nullable=True))


def downgrade():
    """Unapply add bundle_name to ParseImportError."""
    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.drop_column("bundle_name")
