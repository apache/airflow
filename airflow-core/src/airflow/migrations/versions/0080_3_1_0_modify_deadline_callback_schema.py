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
Modify deadline's callback schema.

Revision ID: 808787349f22
Revises: 3bda03debd04
Create Date: 2025-07-31 19:35:53.150465

"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy_jsonfield
from alembic import op

# revision identifiers, used by Alembic.
revision = "808787349f22"
down_revision = "3bda03debd04"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Replace deadline table's string callback and JSON callback_kwargs with JSON callback."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_column("callback")
        batch_op.drop_column("callback_kwargs")
        batch_op.add_column(sa.Column("callback", sqlalchemy_jsonfield.jsonfield.JSONField(), nullable=False))


def downgrade():
    """Replace deadline table's JSON callback with string callback and JSON callback_kwargs."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_column("callback")
        batch_op.add_column(
            sa.Column("callback_kwargs", sqlalchemy_jsonfield.jsonfield.JSONField(), nullable=True)
        )
        batch_op.add_column(sa.Column("callback", sa.String(length=500), nullable=False))
