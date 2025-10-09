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
Add url template and template params to DagBundleModel.

Revision ID: 3bda03debd04
Revises: f56f68b9e02f
Create Date: 2025-07-04 10:12:12.711292

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils import JSONType

# revision identifiers, used by Alembic.
revision = "3bda03debd04"
down_revision = "f56f68b9e02f"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply Add url and template params to DagBundleModel."""
    with op.batch_alter_table("dag_bundle", schema=None) as batch_op:
        batch_op.add_column(sa.Column("signed_url_template", sa.String(length=200), nullable=True))
        batch_op.add_column(sa.Column("template_params", JSONType(), nullable=True))


def downgrade():
    """Unapply Add url and template params to DagBundleModel."""
    with op.batch_alter_table("dag_bundle", schema=None) as batch_op:
        batch_op.drop_column("template_params")
        batch_op.drop_column("signed_url_template")
