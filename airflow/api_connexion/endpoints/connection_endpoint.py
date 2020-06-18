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

from flask import request
from sqlalchemy import func

from airflow.api_connexion import parameters
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.schemas.connection_schema import (
    ConnectionCollection, connection_collection_item_schema, connection_collection_schema,
)
from airflow.models import Connection
from airflow.utils.session import provide_session


def delete_connection():
    """
    Delete a connection entry
    """
    raise NotImplementedError("Not implemented yet.")


@provide_session
def get_connection(connection_id, session):
    """
    Get a connection entry
    """
    query = session.query(Connection)
    query = query.filter(Connection.conn_id == connection_id)
    connection = query.one_or_none()
    if connection is None:
        raise NotFound("Connection not found")
    return connection_collection_item_schema.dump(connection)


@provide_session
def get_connections(session):
    """
    Get all connection entries
    """
    offset = request.args.get(parameters.page_offset, 0)
    limit = min(int(request.args.get(parameters.page_limit, 100)), 100)

    total_entries = session.query(func.count(Connection.id)).scalar()
    query = session.query(Connection).order_by(Connection.id).offset(offset).limit(limit)

    connections = query.all()
    return connection_collection_schema.dump(ConnectionCollection(connections=connections,
                                                                  total_entries=total_entries))


def patch_connection():
    """
    Update a connection entry
    """
    raise NotImplementedError("Not implemented yet.")


def post_connection():
    """
    Create connection entry
    """
    raise NotImplementedError("Not implemented yet.")
