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
"""Connection sub-commands"""
import sys
from typing import List
from urllib.parse import urlunparse

from sqlalchemy.orm import exc
from tabulate import tabulate

from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from airflow.utils import cli as cli_utils
from airflow.utils.session import create_session


def _tabulate_connection(conns: List[Connection], tablefmt: str):
    tabulate_data = [
        {
            'Conn Id': conn.conn_id,
            'Conn Type': conn.conn_type,
            'Host': conn.host,
            'Port': conn.port,
            'Is Encrypted': conn.is_encrypted,
            'Is Extra Encrypted': conn.is_encrypted,
            'Extra': conn.extra,
        } for conn in conns
    ]

    msg = tabulate(tabulate_data, tablefmt=tablefmt, headers='keys')
    return msg


def connections_list(args):
    """Lists all connections at the command line"""
    with create_session() as session:
        if args.include_secrets:
            if not args.conn_id:
                print(
                    "To use the '--include-secrets' option, you must also pass '--conn-id' option.",
                    file=sys.stderr
                )
                sys.exit(1)
            conns = BaseHook.get_connections(args.conn_id)
        else:
            query = session.query(Connection)
            if args.conn_id:
                query = query.filter(Connection.conn_id == args.conn_id)
            conns = query.all()

        tablefmt = args.output
        msg = _tabulate_connection(conns, tablefmt)
        print(msg)


alternative_conn_specs = ['conn_type', 'conn_host',
                          'conn_login', 'conn_password', 'conn_schema', 'conn_port']


@cli_utils.action_logging
def connections_add(args):
    """Adds new connection"""
    # Check that the conn_id and conn_uri args were passed to the command:
    missing_args = []
    invalid_args = []
    if args.conn_uri:
        for arg in alternative_conn_specs:
            if getattr(args, arg) is not None:
                invalid_args.append(arg)
    elif not args.conn_type:
        missing_args.append('conn-uri or conn-type')
    if missing_args:
        msg = ('The following args are required to add a connection:' +
               ' {missing!r}'.format(missing=missing_args))
        raise SystemExit(msg)
    if invalid_args:
        msg = ('The following args are not compatible with the ' +
               'add flag and --conn-uri flag: {invalid!r}')
        msg = msg.format(invalid=invalid_args)
        raise SystemExit(msg)

    if args.conn_uri:
        new_conn = Connection(conn_id=args.conn_id, uri=args.conn_uri)
    else:
        new_conn = Connection(conn_id=args.conn_id,
                              conn_type=args.conn_type,
                              host=args.conn_host,
                              login=args.conn_login,
                              password=args.conn_password,
                              schema=args.conn_schema,
                              port=args.conn_port)
    if args.conn_extra is not None:
        new_conn.set_extra(args.conn_extra)

    with create_session() as session:
        if not (session.query(Connection)
                .filter(Connection.conn_id == new_conn.conn_id).first()):
            session.add(new_conn)
            msg = '\n\tSuccessfully added `conn_id`={conn_id} : {uri}\n'
            msg = msg.format(conn_id=new_conn.conn_id,
                             uri=args.conn_uri or
                             urlunparse((args.conn_type,
                                         '{login}:{password}@{host}:{port}'
                                             .format(login=args.conn_login or '',
                                                     password='******' if args.conn_password else '',
                                                     host=args.conn_host or '',
                                                     port=args.conn_port or ''),
                                         args.conn_schema or '', '', '', '')))
            print(msg)
        else:
            msg = '\n\tA connection with `conn_id`={conn_id} already exists\n'
            msg = msg.format(conn_id=new_conn.conn_id)
            print(msg)


@cli_utils.action_logging
def connections_delete(args):
    """Deletes connection from DB"""
    with create_session() as session:
        try:
            to_delete = (session
                         .query(Connection)
                         .filter(Connection.conn_id == args.conn_id)
                         .one())
        except exc.NoResultFound:
            msg = '\n\tDid not find a connection with `conn_id`={conn_id}\n'
            msg = msg.format(conn_id=args.conn_id)
            print(msg)
            return
        except exc.MultipleResultsFound:
            msg = ('\n\tFound more than one connection with ' +
                   '`conn_id`={conn_id}\n')
            msg = msg.format(conn_id=args.conn_id)
            print(msg)
            return
        else:
            deleted_conn_id = to_delete.conn_id
            session.delete(to_delete)
            msg = '\n\tSuccessfully deleted `conn_id`={conn_id}\n'
            msg = msg.format(conn_id=deleted_conn_id)
            print(msg)
