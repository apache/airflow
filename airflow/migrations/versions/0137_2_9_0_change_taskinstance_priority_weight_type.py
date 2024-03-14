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

"""change taskinstance priority weight type

Revision ID: 263b514b6e30
Revises: ab34f260b71c
Create Date: 2024-03-14 22:45:45.481394

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = '263b514b6e30'
down_revision = 'ab34f260b71c'
branch_labels = None
depends_on = None
airflow_version = '2.9.0'


def upgrade():
    """Apply change taskinstance priority weight type"""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.alter_column("priority_weight", existing_type=sa.INTEGER(), type_=sa.Float())


def downgrade():
    """Unapply change taskinstance priority weight type"""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.alter_column("priority_weight", existing_type=sa.Float(), type_=sa.INTEGER())
