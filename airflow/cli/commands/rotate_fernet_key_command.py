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
"""Rotate Fernet key command."""

from __future__ import annotations

from sqlalchemy import select

from airflow.models import Connection, Trigger, Variable
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import is_sqlalchemy_v1


@cli_utils.action_cli
@providers_configuration_loaded
def rotate_fernet_key(args):
    """Rotates all encrypted connection credentials, triggers and variables."""
    batch_size = 100
    rotate_method = rotate_items_in_batches_v1 if is_sqlalchemy_v1() else rotate_items_in_batches_v2
    with create_session() as session:
        with session.begin():  # Start a single transaction
            rotate_method(
                session,
                Connection,
                filter_condition=Connection.is_encrypted | Connection.is_extra_encrypted,
                batch_size=batch_size,
            )
            rotate_method(session, Variable, filter_condition=Variable.is_encrypted, batch_size=batch_size)
            rotate_method(session, Trigger, filter_condition=None, batch_size=batch_size)


def rotate_items_in_batches_v1(session, model_class, filter_condition=None, batch_size=100):
    """
    Rotates Fernet keys for items of a given model in batches to avoid excessive memory usage.

    This function is a replacement for yield_per, which is not available in SQLAlchemy 1.x.
    """
    offset = 0
    while True:
        query = select(model_class)
        if filter_condition is not None:
            query = query.where(filter_condition)
        query = query.offset(offset).limit(batch_size)
        items = session.scalars(query).all()
        if not items:
            break  # No more items to process
        for item in items:
            item.rotate_fernet_key()
        offset += batch_size


def rotate_items_in_batches_v2(session, model_class, filter_condition=None, batch_size=100):
    """
    Rotates Fernet keys for items of a given model in batches to avoid excessive memory usage.

    This function is taking advantage of yield_per available in SQLAlchemy 2.x.
    """
    while True:
        query = select(model_class)
        if filter_condition is not None:
            query = query.where(filter_condition)
        items = session.scalars(query).yield_per(batch_size)
        for item in items:
            item.rotate_fernet_key()
