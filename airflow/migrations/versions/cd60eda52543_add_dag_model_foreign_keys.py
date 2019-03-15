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

"""add dag_model_foreign_keys

Revision ID: cd60eda52543
Revises: 939bb1e647c8
Create Date: 2019-03-15 15:17:15.699867

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "cd60eda52543"
down_revision = "939bb1e647c8"
branch_labels = None
depends_on = None


def upgrade():
    op.create_foreign_key(
        None, "dag_run", "dag", ["dag_id"], ["dag_id"], ondelete="CASCADE"
    )
    op.create_foreign_key(
        None, "task_instance", "dag", ["dag_id"], ["dag_id"], ondelete="CASCADE"
    )


def downgrade():
    op.drop_constraint(None, "task_instance", type_="foreignkey")
    op.drop_constraint(None, "dag_run", type_="foreignkey")
