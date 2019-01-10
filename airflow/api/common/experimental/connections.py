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

from sqlalchemy.orm import exc
from airflow import settings
from airflow.exceptions import MissingArgument, ConnectionNotFound, MultipleConnectionsFound, \
    IncompatibleArgument
from airflow.models.connection import Connection


def add_connection(
    conn_id,
    conn_uri=None,
    conn_type=None,
    conn_host=None,
    conn_login=None,
    conn_password=None,
    conn_schema=None,
    conn_port=None,
    conn_extra=None,
):
    # Check that the conn_id and conn_uri args were passed to the command:
    missing_args = list()
    invalid_args = list()
    if not conn_id:
        missing_args.append('conn_id')

    if conn_uri:
        if conn_type:
            invalid_args.append(conn_type)
        if conn_host:
            invalid_args.append(conn_host)
        if conn_login:
            invalid_args.append(conn_login)
        if conn_password:
            invalid_args.append(conn_password)
        if conn_schema:
            invalid_args.append(conn_schema)
        if conn_port:
            invalid_args.append(conn_port)
    elif not conn_type:
        missing_args.append('conn_uri or conn_type')

    if missing_args:
        msg = ('The following args are required to add a connection:' +
               ' {missing!r}'.format(missing=missing_args))
        raise MissingArgument(msg)
    if invalid_args:
        msg = ('The following args are not compatible with the ' +
               '--add flag and --conn_uri flag: {invalid!r}')
        msg = msg.format(invalid=invalid_args)
        raise MissingArgument(msg)

    if conn_uri:
        new_conn = Connection(conn_id=conn_id, uri=conn_uri)
    else:
        new_conn = Connection(conn_id=conn_id,
                              conn_type=conn_type,
                              host=conn_host,
                              login=conn_login,
                              password=conn_password,
                              schema=conn_schema,
                              port=conn_port)
    if conn_extra is not None:
        new_conn.set_extra(conn_extra)

    session = settings.Session()
    existing_connections = (session
                            .query(Connection)
                            .filter(Connection.conn_id == conn_id)).all()

    if not all(c.conn_type == new_conn.conn_type for c in existing_connections):
        raise IncompatibleArgument('Connections with the same id must all be of the same type')

    session.add(new_conn)
    session.commit()
    return new_conn


def delete_connection(conn_id, delete_all=False):

    if conn_id is None:
        raise MissingArgument('To delete a connection, you must provide a value for ' +
                              'the --conn_id flag.')

    session = settings.Session()
    to_delete = (session
                 .query(Connection)
                 .filter(Connection.conn_id == conn_id)).all()

    if len(to_delete) == 1:
        deleted_conn_id = to_delete[0].conn_id
        for conn in to_delete:
            session.delete(conn)
        session.commit()
        msg = 'Successfully deleted `conn_id`={conn_id}'
        msg = msg.format(conn_id=deleted_conn_id)
        return msg
    elif len(to_delete) > 1:
        if delete_all:
            deleted_conn_id = to_delete[0].conn_id
            num_conns = len(to_delete)
            for conn in to_delete:
                session.delete(conn)
            session.commit()

            msg = 'Successfully deleted {num_conns} connections with `conn_id`={conn_id}'
            msg = msg.format(conn_id=deleted_conn_id, num_conns=num_conns)
            return msg
        else:
            msg = ('Found {num_conns} connection with ' +
                   '`conn_id`={conn_id}. Specify `delete_all=True` to remove all')
            msg = msg.format(conn_id=conn_id, num_conns=len(to_delete))
            return msg
    elif len(to_delete) == 0:
        msg = 'Did not find a connection with `conn_id`={conn_id}'
        msg = msg.format(conn_id=conn_id)
        return msg


def list_connections():
    session = settings.Session()
    return session.query(Connection).all()


def update_connection(conn_id,
                      conn_uri=None,
                      conn_type=None,
                      conn_host=None,
                      conn_login=None,
                      conn_password=None,
                      conn_schema=None,
                      conn_port=None,
                      conn_extra=None):
    # Check that the conn_id and conn_uri args were passed to the command:
    missing_args = list()
    invalid_args = list()
    if not conn_id:
        missing_args.append('conn_id')
    if conn_uri:
        if conn_type:
            invalid_args.append(conn_type)
        if conn_host:
            invalid_args.append(conn_host)
        if conn_login:
            invalid_args.append(conn_login)
        if conn_password:
            invalid_args.append(conn_password)
        if conn_schema:
            invalid_args.append(conn_schema)
        if conn_port:
            invalid_args.append(conn_port)
    elif not conn_type:
        missing_args.append('conn_uri or conn_type')

    if missing_args:
        msg = ('The following args are required to update a connection:' +
               ' {missing!r}'.format(missing=missing_args))
        raise MissingArgument(msg)
    if invalid_args:
        msg = ('The following args are not compatible with the ' +
               '--update flag and --conn_uri flag: {invalid!r}')
        msg = msg.format(invalid=invalid_args)
        raise MissingArgument(msg)

    # Update....
    session = settings.Session()
    try:
        to_update = (session
                     .query(Connection)
                     .filter(Connection.conn_id == conn_id)
                     .one())
    except exc.NoResultFound:
        msg = 'Did not find a connection with `conn_id`={conn_id}'
        msg = msg.format(conn_id=conn_id)
        raise ConnectionNotFound(msg)
    except exc.MultipleResultsFound:
        msg = ('Updating multiple connections is not supported, Found multiple connections with ' +
               '`conn_id`={conn_id}')
        msg = msg.format(conn_id=conn_id)
        raise MultipleConnectionsFound(msg)
    else:

        # build a new connection to update from
        if conn_uri:
            temp_conn = Connection(conn_id='new_conn', uri=conn_uri)
        else:
            temp_conn = Connection(conn_id='new_conn',
                                   conn_type=conn_type,
                                   host=conn_host,
                                   login=conn_login,
                                   password=conn_password,
                                   schema=conn_schema,
                                   port=conn_port)
        if conn_extra is not None:
            temp_conn.set_extra(conn_extra)

        to_update.conn_type = temp_conn.conn_type or to_update.conn_type
        to_update.host = temp_conn.host or to_update.host
        to_update.login = temp_conn.login or to_update.login
        to_update.password = temp_conn.password or to_update.password
        to_update.schema = temp_conn.schema or to_update.schema
        to_update.port = temp_conn.port or to_update.port

        if temp_conn.extra is not None:
            to_update.set_extra(temp_conn.extra)
        session.commit()

        return to_update
