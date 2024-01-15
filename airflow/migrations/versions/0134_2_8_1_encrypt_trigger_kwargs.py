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

"""encrypt trigger kwargs

Revision ID: 1949afb29106
Revises: 88344c1d9134
Create Date: 2024-01-15 23:55:09.406395

"""
from typing import Any, Callable

import sqlalchemy as sa
from airflow.models.crypto import get_fernet

from airflow.models.trigger import Trigger
from alembic import op


# revision identifiers, used by Alembic.
revision = "1949afb29106"
down_revision = "88344c1d9134"
branch_labels = None
depends_on = None
airflow_version = "2.8.1"


def fernet_operation(_value: Any, operation: Callable[[bytes], bytes]) -> Any:
    if isinstance(_value, str):
        return operation(_value.encode("utf-8")).decode("utf-8")
    if isinstance(_value, dict):
        return {k: fernet_operation(v, operation) for k, v in _value.items()}
    if isinstance(_value, list):
        return [fernet_operation(v, operation) for v in _value]
    if isinstance(_value, tuple):
        return tuple(fernet_operation(v, operation) for v in _value)
    return _value


def get_session() -> sa.orm.Session:
    conn = op.get_bind()
    sessionmaker = sa.orm.sessionmaker()
    return sessionmaker(bind=conn)


def apply_fernet_operation(operation: Callable[[bytes], bytes]) -> None:
    session = get_session()
    try:
        for trigger in session.query(Trigger).all():
            trigger.kwargs = fernet_operation(trigger.kwargs, operation)
        session.commit()
    finally:
        session.close()


def upgrade():
    """Apply encrypt trigger kwargs"""
    apply_fernet_operation(get_fernet().encrypt())


def downgrade():
    """Unapply encrypt trigger kwargs"""
    apply_fernet_operation(get_fernet().decrypt())
