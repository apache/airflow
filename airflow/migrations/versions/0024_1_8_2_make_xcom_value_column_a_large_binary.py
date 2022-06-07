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

"""Make xcom value column a large binary

Revision ID: bdaa763e6c56
Revises: cc1e65623dc7
Create Date: 2017-08-14 16:06:31.568971

"""
import dill
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'bdaa763e6c56'
down_revision = 'cc1e65623dc7'
branch_labels = None
depends_on = None
airflow_version = '1.8.2'


def upgrade():
    # There can be data truncation here as LargeBinary can be smaller than the pickle
    # type.
    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table("xcom") as batch_op:
        batch_op.alter_column('value', type_=sa.LargeBinary())


def downgrade():
    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table("xcom") as batch_op:
        batch_op.alter_column('value', type_=sa.PickleType(pickler=dill))
