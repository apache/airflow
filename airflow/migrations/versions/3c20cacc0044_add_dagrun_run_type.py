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
Add DagRun run_type

Revision ID: 3c20cacc0044
Revises: b25a55525161
Create Date: 2020-04-08 13:35:25.671327

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Boolean, Column, Integer, PickleType, String
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative import declarative_base

from airflow.models.base import ID_LEN
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# revision identifiers, used by Alembic.
revision = "3c20cacc0044"
down_revision = "b25a55525161"
branch_labels = None
depends_on = None

Base = declarative_base()


class DagRun(Base):  # type: ignore
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """
    __tablename__ = "dag_run"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    execution_date = Column(UtcDateTime, default=timezone.utcnow)
    start_date = Column(UtcDateTime, default=timezone.utcnow)
    end_date = Column(UtcDateTime)
    _state = Column('state', String(50), default=State.RUNNING)
    run_id = Column(String(ID_LEN))
    external_trigger = Column(Boolean, default=True)
    run_type = Column(String(50), nullable=False)
    conf = Column(PickleType)


def upgrade():
    """Apply Add DagRun run_type"""
    run_type_col_type = sa.String(length=50)

    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    dag_run_columns = [col.get('name') for col in inspector.get_columns("dag_run")]

    if "run_type" not in dag_run_columns:

        # Add nullable column
        with op.batch_alter_table("dag_run") as batch_op:
            batch_op.add_column(sa.Column("run_type", run_type_col_type, nullable=True))

        # Generate run type for existing records
        sessionmaker = sa.orm.sessionmaker()
        session = sessionmaker(bind=conn)

        for run_type in DagRunType:
            session.query(DagRun).filter(DagRun.run_id.like(f"{run_type.value}__%")).update(
                {DagRun.run_type: run_type.value}, synchronize_session=False
            )

        session.query(DagRun).filter(DagRun.run_type.is_(None)).update(
            {DagRun.run_type: DagRunType.MANUAL.value}, synchronize_session=False
        )
        session.commit()

        # Make run_type not nullable
        with op.batch_alter_table("dag_run") as batch_op:
            batch_op.alter_column("run_type", type_=run_type_col_type, nullable=False)


def downgrade():
    """Unapply Add DagRun run_type"""
    op.drop_column("run_type")
