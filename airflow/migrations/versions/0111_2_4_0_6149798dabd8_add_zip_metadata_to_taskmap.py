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

"""Add zip metadata to TaskMap.

Revision ID: 6149798dabd8
Revises: 3c94c427fdf6
Create Date: 2022-06-08 08:03:31.242461
"""

from alembic import op
from sqlalchemy import Column, Integer

# Revision identifiers, used by Alembic.
revision = "6149798dabd8"
down_revision = "3c94c427fdf6"
branch_labels = None
depends_on = None

airflow_version = "2.4.0"


def upgrade():
    """Add zip metadata to TaskMap."""
    with op.batch_alter_table("task_map") as batch_op:
        batch_op.add_column(Column("zip_length", Integer, nullable=True))
        batch_op.add_column(Column("zip_longest_length", Integer, nullable=True))


def downgrade():
    """Remove zip metadata from TaskMap."""
    with op.batch_alter_table("task_map") as batch_op:
        batch_op.drop_column("zip_length")
        batch_op.drop_column("zip_longest_length")
