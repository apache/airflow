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
Rename filename to source_reference in import_error.

Revision ID: ca8499dc1004
Revises: f8c2a1d94e03
Create Date: 2026-08-27 12:45:02.276898

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ca8499dc1004"
down_revision = "f8c2a1d94e03"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Apply rename filename to source_reference in import_error."""
    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.alter_column("filename", new_column_name="source_reference", type_=sa.String(length=1024))


def downgrade():
    """Unapply rename filename to source_reference in import_error."""
    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.alter_column("source_reference", new_column_name="filename", type_=sa.String(length=1024))
