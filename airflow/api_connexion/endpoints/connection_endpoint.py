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

from connexion import NoContent
from flask import request
from sqlalchemy import func

from airflow.api_connexion.exceptions import AlreadyExists, NotFound
from airflow.api_connexion.schemas.connection_schema import (
    ConnectionCollection, connection_collection_item_schema, connection_collection_schema, connection_schema,
)
from airflow.models import Connection
from airflow.utils.session import provide_session
from airflow.www.app import csrf


@provide_session
@csrf.exempt
def delete_connection(connection_id, session):
    """
    Delete a connection entry
    """
    query = session.query(Connection)
    query = query.filter_by(conn_id=connection_id)
    connection = query.one_or_none()
    if connection is None:
        raise NotFound('Connection not found')
    session.delete(connection)
    return NoContent, 204


@provide_session
def get_connection(connection_id, session):
    """
    Get a connection entry
    """
    connection = session.query(Connection).filter(Connection.conn_id == connection_id).one_or_none()
    if connection is None:
        raise NotFound("Connection not found")
    return connection_collection_item_schema.dump(connection)


@provide_session
def get_connections(session, limit, offset=0):
    """
    Get all connection entries
    """
    total_entries = session.query(func.count(Connection.id)).scalar()
    query = session.query(Connection)
    connections = query.offset(offset).limit(limit).all()
    return connection_collection_schema.dump(ConnectionCollection(connections=connections,
                                                                  total_entries=total_entries))


@provide_session
@csrf.exempt
def patch_connection(connection_id, session, update_mask=None):
    """
    Update a connection entry
    """
    body = request.json
    connection = session.query(Connection).filter_by(conn_id=connection_id).first()
    if connection is None:
        raise NotFound("Connection not found")
    if update_mask:
        for field in update_mask:
            setattr(connection, field, body[field])
    else:
        for key in body:
            setattr(connection, key, body[key])
    session.add(connection)
    session.commit()
    return connection_schema.dump(connection)


@provide_session
@csrf.exempt
def post_connection(session):
    """
    Create connection entry
    """
    body = request.json

    # connexion handles 404, no need for try/except
    result = connection_schema.load(body)
    data = result.data
    conn_id = data['conn_id']
    query = session.query(Connection)
    connection = query.filter_by(conn_id=conn_id).first()
    if not connection:
        connection = Connection(**data)
        session.add(connection)
        session.commit()
        return connection_schema.dump(connection)
    raise AlreadyExists("Connection already exist. ID: %s" % conn_id)
