# -*- coding: utf-8 -*-
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

from airflow.exceptions import AirflowBadRequest, ConnectionNotFound
from airflow.models import Connection
from airflow.utils.db import provide_session


@provide_session
def get_connection(conn_id, session=None):
    """Get connection by a given ID."""
    if not (conn_id and conn_id.strip()):
        raise AirflowBadRequest("Connection ID shouldn't be empty")

    connection = session.query(Connection).filter_by(conn_id=conn_id).first()
    if connection is None:
        raise ConnectionNotFound("Connection '%s' doesn't exist" % conn_id)

    return connection


@provide_session
def get_connections(session=None):
    """Get all connections."""
    return session.query(Connection).all()


@provide_session
def create_connection(conn_id, session=None, **kwargs):
    """Create a connection with the given parameters."""
    if not (conn_id and conn_id.strip()):
        raise AirflowBadRequest("Connection ID shouldn't be empty")

    session.expire_on_commit = False
    connection = session.query(Connection).filter_by(conn_id=conn_id).first()
    if connection is None:
        connection = Connection(conn_id=conn_id, **kwargs)
        session.add(connection)
    else:
        for key, value in kwargs.items():
            setattr(connection, key, value)

    session.commit()

    return connection


@provide_session
def delete_connection(conn_id, session=None):
    """Delete connection by a given ID."""
    if not (conn_id and conn_id.strip()):
        raise AirflowBadRequest("Connection ID shouldn't be empty")

    connection = session.query(Connection).filter_by(conn_id=conn_id).first()
    if connection is None:
        raise ConnectionNotFound("Connection '%s' doesn't exist" % conn_id)

    session.delete(connection)
    session.commit()

    return connection
