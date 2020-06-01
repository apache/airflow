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

import connexion

from airflow.api_connexion import parameters
from airflow.api_connexion.schemas.connection import (
    connection_collection_item_schema, connection_collection_schema, connection_schema,
)
from airflow.models import Connection
from airflow.utils.session import provide_session


@provide_session
def get_connection(connection_id, session):
    """
    Get a connection entry
    """
    query = session.query(Connection)
    query = query.filter(Connection.conn_id == connection_id)
    connection = query.one_or_none()
    return connection_collection_item_schema.dump(connection), 200


@provide_session
def get_connections(session):
    """
    Get all connection entries
    """
    request = connexion.request
    offset = request.args.get(parameters.page_offset, 0)
    limit = min(request.args.get(parameters.page_limit, 100), 100)

    query = session.query(Connection)
    query = query.offset(offset).limit(limit)

    connections = query.all()
    return connection_collection_schema.dump(connections), 200


@provide_session
def patch_connection(connection_id, session):
    """
    Update a connection entry
    """
    request = connexion.request
    update_mask = request.args.get('update_mask')
    body = request.json

    query = session.query(Connection)
    query = query.filter(Connection.conn_id == connection_id)
    connection = query.one_or_none()
    for field in update_mask:
        if hasattr(connection, field):
            setattr(connection, field, body[field])
    session.merge(connection)
    session.commit()

    return connection_schema.dump(connection), 200


@provide_session
def post_connection(session):
    """
    Create connection entry
    """
    request = connexion.request
    body = request.json
    connection = Connection(**body)
    session.add(connection)
    session.commit()
    return connection_schema.dump(connection), 200


@provide_session
def delete_connection(connection_id, session):
    """
    Delete a connection entry
    """
    query = session.query(Connection)
    query = query.filter(Connection.conn_id == connection_id)
    connection = query.first()
    session.delete(connection)
    return 'No Content', 204
