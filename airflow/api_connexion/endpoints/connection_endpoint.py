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

import os

from flask import request
from marshmallow import ValidationError
from sqlalchemy import func

from airflow.api_connexion import security
from airflow.api_connexion.endpoints.base import ModelEndpoints
from airflow.api_connexion.exceptions import BadRequest
from airflow.api_connexion.schemas.connection_schema import (
    ConnectionCollection,
    connection_collection_schema,
    connection_schema,
    connection_test_schema,
)
from airflow.models import Connection
from airflow.secrets.environment_variables import CONN_ENV_PREFIX
from airflow.security import permissions
from airflow.utils.session import provide_session
from airflow.utils.strings import get_random_string


class ConnectionEndpoints(ModelEndpoints[Connection]):
    """Endpoint implementation generator for Connection."""

    model = Connection

    collection_schema = connection_collection_schema
    instance_schema = connection_schema
    protected_fields = ["conn_id"]

    internal_external_lookups = {"conn_id": "connection_id"}

    orderings = {
        "id": Connection.id,
        "conn_id": Connection.conn_id,
        "connection_id": Connection.conn_id,
        "conn_type": Connection.conn_type,
        "description": Connection.description,
        "host": Connection.host,
        "port": Connection.port,
    }
    primary_key = {"conn_id": Connection.conn_id}

    def prepare_collection_for_dump(self, collection):
        total_entries = self.session.query(func.count(self.model.id)).scalar()
        return ConnectionCollection(collection, total_entries)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
@provide_session
def get_connections(session, limit, offset=0, order_by="id"):
    return ConnectionEndpoints(session).get_collection_view(limit=limit, offset=offset, order_by=order_by)


@security.requires_access([(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_CONNECTION)])
@provide_session
def delete_connection(connection_id, session):
    return ConnectionEndpoints(session).delete_instance_view(conn_id=connection_id)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
@provide_session
def get_connection(connection_id, session):
    return ConnectionEndpoints(session).get_instance_view(conn_id=connection_id)


@security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_CONNECTION)])
@provide_session
def patch_connection(connection_id, session, update_mask=None):
    return ConnectionEndpoints(session).edit_instance_view(update_mask, conn_id=connection_id)


@security.requires_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION)])
@provide_session
def post_connection(session):
    return ConnectionEndpoints(session).create_instance_view()


@security.requires_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION)])
def test_connection():
    """
    To test a connection, this method first creates an in-memory dummy conn_id & exports that to an
    env var, as some hook classes tries to find out the conn from their __init__ method & errors out
    if not found. It also deletes the conn id env variable after the test.
    """
    body = request.json
    dummy_conn_id = get_random_string()
    conn_env_var = f'{CONN_ENV_PREFIX}{dummy_conn_id.upper()}'
    try:
        data = connection_schema.load(body)
        data['conn_id'] = dummy_conn_id
        conn = Connection(**data)
        os.environ[conn_env_var] = conn.get_uri()
        status, message = conn.test_connection()
        return connection_test_schema.dump({"status": status, "message": message})
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    finally:
        if conn_env_var in os.environ:
            del os.environ[conn_env_var]
