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
Add source_reference to import_error.

Revision ID: ca8499dc1004
Revises: c9f4b3e7a218
Create Date: 2026-08-27 12:45:02.276898

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ca8499dc1004"
down_revision = "c9f4b3e7a218"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Apply add source_reference to import_error."""
    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.add_column(sa.Column("source_reference", sa.String(length=2000), nullable=True))


def downgrade():
    """Unapply add source_reference to import_error."""
    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.drop_column("source_reference")
