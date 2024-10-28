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
update trigger kwargs type and encrypt.

Revision ID: 1949afb29106
Revises: ee1467d4aa35
Create Date: 2024-03-17 22:09:09.406395

"""

from __future__ import annotations

import json
from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.orm import lazyload

from airflow.models.trigger import Trigger
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.sqlalchemy import ExtendedJSON

# revision identifiers, used by Alembic.
revision = "1949afb29106"
down_revision = "ee1467d4aa35"
branch_labels = None
depends_on = None
airflow_version = "2.9.0"


def get_session() -> sa.orm.Session:
    conn = op.get_bind()
    sessionmaker = sa.orm.sessionmaker()
    return sessionmaker(bind=conn)


def upgrade():
    """Update trigger kwargs type to string and encrypt."""
    with op.batch_alter_table("trigger") as batch_op:
        batch_op.alter_column("kwargs", type_=sa.Text(), existing_nullable=False)

    if not context.is_offline_mode():
        session = get_session()
        try:
            for trigger in session.query(Trigger).options(
                lazyload(Trigger.task_instance)
            ):
                trigger.kwargs = trigger.kwargs
            session.commit()
        finally:
            session.close()


def downgrade():
    """Unapply update trigger kwargs type to string and encrypt."""
    if context.is_offline_mode():
        print(
            dedent("""
        ------------
        --  WARNING: Unable to decrypt trigger kwargs automatically in offline mode!
        --  If any trigger rows exist when you do an offline downgrade, the migration will fail.
        ------------
        """)
        )
    else:
        session = get_session()
        try:
            for trigger in session.query(Trigger).options(
                lazyload(Trigger.task_instance)
            ):
                trigger.encrypted_kwargs = json.dumps(
                    BaseSerialization.serialize(trigger.kwargs)
                )
            session.commit()
        finally:
            session.close()

    with op.batch_alter_table("trigger") as batch_op:
        batch_op.alter_column(
            "kwargs",
            type_=ExtendedJSON(),
            postgresql_using="kwargs::json",
            existing_nullable=False,
        )
