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
add dataset_expression in DagModel.

Revision ID: ab34f260b71c
Revises: d75389605139
Create Date: 2024-03-07 19:54:38.316059

"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy_jsonfield
from alembic import op

from airflow.settings import json

# revision identifiers, used by Alembic.
revision = "ab34f260b71c"
down_revision = "d75389605139"
branch_labels = None
depends_on = None
airflow_version = "2.9.0"


def upgrade():
    """Apply Add dataset_expression to DagModel."""
    with op.batch_alter_table("dag") as batch_op:
        batch_op.add_column(
            sa.Column("dataset_expression", sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
        )


def downgrade():
    """Unapply Add dataset_expression to DagModel."""
    with op.batch_alter_table("dag") as batch_op:
        batch_op.drop_column("dataset_expression")
