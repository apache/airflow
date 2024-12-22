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
Drop DAG pickling.

Revision ID: d03e4a635aa3
Revises: d8cd3297971e
Create Date: 2024-11-04 22:07:51.329843

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = "d03e4a635aa3"
down_revision = "d8cd3297971e"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Drop DAG pickling."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("pickle_id")
        batch_op.drop_column("last_pickled")

    op.drop_table("dag_pickle")


def downgrade():
    """Re-Add DAG pickling."""
    import dill

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("last_pickled", sa.TIMESTAMP(), nullable=True))
        batch_op.add_column(sa.Column("pickle_id", sa.INTEGER(), nullable=True))

    op.create_table(
        "dag_pickle",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("pickle", sa.PickleType(pickler=dill), nullable=True),
        sa.Column("created_dttm", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("pickle_hash", sa.BigInteger, nullable=True),
    )
