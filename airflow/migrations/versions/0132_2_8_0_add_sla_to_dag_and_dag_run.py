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

"""add sla to dag and dag run

Revision ID: 8ab46618a397
Revises: bd5dfbe21f88
Create Date: 2023-08-19 21:13:05.512281

"""

import json
import sqlalchemy as sa
from alembic import op
from sqlalchemy import Column, delete, Integer, select, String, update
from sqlalchemy.orm import declarative_base
from airflow.utils.sqlalchemy import ExtendedJSON
from airflow.utils.state import DagRunState

Base = declarative_base()


class DbCallbackRequest(Base):  # type: ignore
    """Minimal model definition for migrations"""

    __tablename__ = "callback_request"

    id = Column(Integer(), nullable=False, primary_key=True)
    callback_data = Column(ExtendedJSON, nullable=False)
    callback_type = Column(String(20), nullable=False)


from airflow.models.db_callback_request import DbCallbackRequest


# revision identifiers, used by Alembic.
revision = "8ab46618a397"
down_revision = "bd5dfbe21f88"
branch_labels = None
depends_on = None
airflow_version = "2.8.0"


def upgrade():
    """Apply add sla to dag and dag run"""
    # Add sla_missed column to dag_run
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.add_column(
            sa.Column(
                "sla_missed",
                sa.Boolean(),
                default=False,
                nullable=False,
                server_default="0",
            )
        )

    # Update json data in DbCallbackRequest to use dagrun_state instead of is_failure_callback
    conn = op.get_bind()

    sessionmaker = sa.orm.sessionmaker()
    session = sessionmaker(bind=conn)

    existing = session.execute(
        select(DbCallbackRequest.id, DbCallbackRequest.callback_data).where(
            DbCallbackRequest.callback_type == "DagCallbackRequest"
        )
    ).all()
    for id_, callback_data in existing:
        json_object = json.loads(callback_data)
        if "is_failure_callback" in json_object:
            is_failure_callback = json_object.pop("is_failure_callback")
            if is_failure_callback:
                json_object["dagrun_state"] = DagRunState.FAILED
            else:
                json_object["dagrun_state"] = DagRunState.SUCCESS
            json_data = json.dumps(json_object)

            session.execute(
                update(DbCallbackRequest)
                .where(DbCallbackRequest.id == id_)
                .values(callback_data=json_data)
                .execution_options(synchronize_session=False)
            )
    session.commit()


def downgrade():
    """Unapply add sla to dag and dag run"""
    # Drop sla_missed column from dag_run
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.drop_column("sla_missed")

    # Update json data in DbCallbackRequest to use is_failure_callback instead of dagrun_state
    conn = op.get_bind()

    sessionmaker = sa.orm.sessionmaker()
    session = sessionmaker(bind=conn)

    existing = session.execute(
        select(DbCallbackRequest.id, DbCallbackRequest.callback_data).where(
            DbCallbackRequest.callback_type == "DagCallbackRequest"
        )
    ).all()
    for id_, callback_data in existing:
        json_object = json.loads(callback_data)
        if "dagrun_state" in json_object:
            dagrun_state = json_object.pop("dagrun_state")
            if dagrun_state not in (DagRunState.FAILED, DagRunState.SUCCESS):
                session.execute(
                    delete(DbCallbackRequest)
                    .where(DbCallbackRequest.id == id_)
                    .execution_options(synchronize_session=False)
                )
            else:
                if dagrun_state == DagRunState.FAILED:
                    json_object["is_failure_callback"] = True
                else:
                    json_object["is_failure_callback"] = False
                json_object.pop("sla_miss", None)
                json_data = json.dumps(json_object)
                session.execute(
                    update(DbCallbackRequest)
                    .where(DbCallbackRequest.id == id_)
                    .values(callback_data=json_data)
                    .execution_options(synchronize_session=False)
                )
    session.commit()
