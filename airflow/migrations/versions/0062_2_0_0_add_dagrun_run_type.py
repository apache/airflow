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
Add ``run_type`` column in ``dag_run`` table

Revision ID: 3c20cacc0044
Revises: b25a55525161
Create Date: 2020-04-08 13:35:25.671327

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Column, Integer, String, inspect
from sqlalchemy.orm import declarative_base

from airflow.utils.types import DagRunType

# revision identifiers, used by Alembic.
revision = "3c20cacc0044"
down_revision = "b25a55525161"
branch_labels = None
depends_on = None
airflow_version = "2.0.0"

Base = declarative_base()


class DagRun(Base):  # type: ignore
    """Minimal model definition for migrations"""

    __tablename__ = "dag_run"

    id = Column(Integer, primary_key=True)
    run_id = Column(String())
    run_type = Column(String(50), nullable=False)


def upgrade():
    """Apply Add ``run_type`` column in ``dag_run`` table"""
    run_type_col_type = sa.String(length=50)

    conn = op.get_bind()
    inspector = inspect(conn)
    dag_run_columns = [col.get("name") for col in inspector.get_columns("dag_run")]

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
            batch_op.alter_column(
                "run_type", existing_type=run_type_col_type, type_=run_type_col_type, nullable=False
            )


def downgrade():
    """Unapply Add ``run_type`` column in ``dag_run`` table"""
    op.drop_column("dag_run", "run_type")
