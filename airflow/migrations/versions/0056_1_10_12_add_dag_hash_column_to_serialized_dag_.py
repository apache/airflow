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
Add ``dag_hash`` Column to ``serialized_dag`` table.

Revision ID: da3f683c3a5a
Revises: a66efa278eea
Create Date: 2020-08-07 20:52:09.178296

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "da3f683c3a5a"
down_revision = "a66efa278eea"
branch_labels = None
depends_on = None
airflow_version = "1.10.12"


def upgrade():
    """Apply Add ``dag_hash`` Column to ``serialized_dag`` table."""
    op.add_column(
        "serialized_dag",
        sa.Column("dag_hash", sa.String(32), nullable=False, server_default="Hash not calculated yet"),
    )


def downgrade():
    """Unapply Add ``dag_hash`` Column to ``serialized_dag`` table."""
    op.drop_column("serialized_dag", "dag_hash")
