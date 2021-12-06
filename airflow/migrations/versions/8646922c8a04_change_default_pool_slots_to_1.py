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

"""Change default pool_slots to 1

Revision ID: 8646922c8a04
Revises: 449b4072c2da
Create Date: 2021-02-23 23:19:22.409973

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = '8646922c8a04'
down_revision = '449b4072c2da'
branch_labels = None
depends_on = None

Base = declarative_base()
BATCH_SIZE = 5000


class TaskInstance(Base):  # type: ignore
    """Minimal model definition for migrations"""

    __tablename__ = "task_instance"

    task_id = Column(String(), primary_key=True)
    dag_id = Column(String(), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    pool_slots = Column(Integer, default=1)


def upgrade():
    """Change default pool_slots to 1 and make pool_slots not nullable"""
    connection = op.get_bind()
    sessionmaker = sa.orm.sessionmaker()
    session = sessionmaker(bind=connection)

    session.query(TaskInstance).filter(TaskInstance.pool_slots.is_(None)).update(
        {TaskInstance.pool_slots: 1}, synchronize_session=False
    )
    session.commit()

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("pool_slots", existing_type=sa.Integer, nullable=False)


def downgrade():
    """Unapply Change default pool_slots to 1"""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("pool_slots", existing_type=sa.Integer, nullable=True)
