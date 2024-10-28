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
Add rendered_map_index to TaskInstance.

Revision ID: 1fd565369930
Revises: 88344c1d9134
Create Date: 2024-02-26 18:48:06.386776

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1fd565369930"
down_revision = "88344c1d9134"
branch_labels = None
depends_on = None
airflow_version = "2.9.0"


def upgrade():
    """Apply Add rendered_map_index to TaskInstance."""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.add_column(
            sa.Column("rendered_map_index", sa.String(length=250), nullable=True)
        )


def downgrade():
    """Unapply Add rendered_map_index to TaskInstance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_column("rendered_map_index")
