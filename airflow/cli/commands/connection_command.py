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
"""Connection sub-commands."""
from __future__ import annotations

import io
import json
import os
import sys
import warnings
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import select
from sqlalchemy.orm import exc

from airflow.cli.simple_table import AirflowConsole
from airflow.compat.functools import cache
from airflow.configuration import conf
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers_manager import ProvidersManager
from airflow.secrets.local_filesystem import load_connections_dict
from airflow.utils import cli as cli_utils, helpers, yaml
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.db import create_default_connections as db_create_default_connections
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import create_session


def _connection_mapper(conn: Connection) -> dict[str, Any]:
    return {
        "id": conn.id,
        "conn_id": conn.conn_id,
        "conn_type": conn.conn_type,
        "description": conn.description,
        "host": conn.host,
        "schema": conn.schema,
        "login": conn.login,
        "password": conn.password,
        "port": conn.port,
        "is_encrypted": conn.is_encrypted,
        "is_extra_encrypted": conn.is_encrypted,
        "extra_dejson": conn.extra_dejson,
        "get_uri": conn.get_uri(),
    }


@suppress_logs_and_warning
@providers_configuration_loaded
def connections_get(args):
    """Get a connection."""
    try:
        conn = BaseHook.get_connection(args.conn_id)
    except AirflowNotFoundException:
        raise SystemExit("Connection not found.")
    AirflowConsole().print_as(
        data=[conn],
        output=args.output,
        mapper=_connection_mapper,
    )


@suppress_logs_and_warning
@providers_configuration_loaded
def connections_list(args):
    """List all connections at the command line."""
    with create_session() as session:
        query = select(Connection)
        if args.conn_id:
            query = query.where(Connection.conn_id == args.conn_id)
        query = session.scalars(query)
        conns = query.all()

        AirflowConsole().print_as(
            data=conns,
            output=args.output,
            mapper=_connection_mapper,
        )


def _connection_to_dict(conn: Connection) -> dict:
    return dict(
        conn_type=conn.conn_type,
        description=conn.description,
        login=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port,
        schema=conn.schema,
        extra=conn.extra,
    )


def create_default_connections(args):
    db_create_default_connections()


def _format_connections(conns: list[Connection], file_format: str, serialization_format: str) -> str:
    if serialization_format == "json":
        serializer_func = lambda x: json.dumps(_connection_to_dict(x))
    elif serialization_format == "uri":
        serializer_func = Connection.get_uri
    else:
        raise SystemExit(f"Received unexpected value for `--serialization-format`: {serialization_format!r}")
    if file_format == ".env":
        connections_env = ""
        for conn in conns:
            connections_env += f"{conn.conn_id}={serializer_func(conn)}\n"
        return connections_env

    connections_dict = {}
    for conn in conns:
        connections_dict[conn.conn_id] = _connection_to_dict(conn)

    if file_format == ".yaml":
        return yaml.dump(connections_dict)

    if file_format == ".json":
        return json.dumps(connections_dict, indent=2)

    return json.dumps(connections_dict)


def _is_stdout(fileio: io.TextIOWrapper) -> bool:
    return fileio.name == "<stdout>"


def _valid_uri(uri: str) -> bool:
    """Check if a URI is valid, by checking if scheme (conn_type) provided."""
    return urlsplit(uri).scheme != ""


@cache
def _get_connection_types() -> list[str]:
    """Return connection types available."""
    _connection_types = ["fs", "mesos_framework-id", "email", "generic"]
    providers_manager = ProvidersManager()
    for connection_type, provider_info in providers_manager.hooks.items():
        if provider_info:
            _connection_types.append(connection_type)
    return _connection_types


@providers_configuration_loaded
def connections_export(args):
    """Export all connections to a file."""
    file_formats = [".yaml", ".json", ".env"]
    if args.format:
        warnings.warn("Option `--format` is deprecated.  Use `--file-format` instead.", DeprecationWarning)
    if args.format and args.file_format:
        raise SystemExit("Option `--format` is deprecated.  Use `--file-format` instead.")
    default_format = ".json"
    provided_file_format = None
    if args.format or args.file_format:
        provided_file_format = f".{(args.format or args.file_format).lower()}"

    file_is_stdout = _is_stdout(args.file)
    if file_is_stdout:
        filetype = provided_file_format or default_format
    elif provided_file_format:
        filetype = provided_file_format
    else:
        filetype = Path(args.file.name).suffix
        filetype = filetype.lower()
        if filetype not in file_formats:
            raise SystemExit(
                f"Unsupported file format. The file must have the extension {', '.join(file_formats)}."
            )

    if args.serialization_format and not filetype == ".env":
        raise SystemExit("Option `--serialization-format` may only be used with file type `env`.")

    with create_session() as session:
        connections = session.scalars(select(Connection).order_by(Connection.conn_id)).all()

    msg = _format_connections(
        conns=connections,
        file_format=filetype,
        serialization_format=args.serialization_format or "uri",
    )

    with args.file as f:
        f.write(msg)

    if file_is_stdout:
        print("\nConnections successfully exported.", file=sys.stderr)
    else:
        print(f"Connections successfully exported to {args.file.name}.")


alternative_conn_specs = ["conn_type", "conn_host", "conn_login", "conn_password", "conn_schema", "conn_port"]


@cli_utils.action_cli
@providers_configuration_loaded
def connections_add(args):
    """Add new connection."""
    has_uri = bool(args.conn_uri)
    has_json = bool(args.conn_json)
    has_type = bool(args.conn_type)

    # Validate connection-id
    try:
        helpers.validate_key(args.conn_id, max_length=200)
    except Exception as e:
        raise SystemExit(f"Could not create connection. {e}")

    if not has_type and not (has_json or has_uri):
        raise SystemExit("Must supply either conn-uri or conn-json if not supplying conn-type")

    if has_json and has_uri:
        raise SystemExit("Cannot supply both conn-uri and conn-json")

    if has_type and args.conn_type not in _get_connection_types():
        warnings.warn(f"The type provided to --conn-type is invalid: {args.conn_type}")
        warnings.warn(
            f"Supported --conn-types are:{_get_connection_types()}."
            "Hence overriding the conn-type with generic"
        )
        args.conn_type = "generic"

    if has_uri or has_json:
        invalid_args = []
        if has_uri and not _valid_uri(args.conn_uri):
            raise SystemExit(f"The URI provided to --conn-uri is invalid: {args.conn_uri}")

        for arg in alternative_conn_specs:
            if getattr(args, arg) is not None:
                invalid_args.append(arg)

        if has_json and args.conn_extra:
            invalid_args.append("--conn-extra")

        if invalid_args:
            raise SystemExit(
                "The following args are not compatible with "
                f"the --conn-{'uri' if has_uri else 'json'} flag: {invalid_args!r}"
            )

    if args.conn_uri:
        new_conn = Connection(conn_id=args.conn_id, description=args.conn_description, uri=args.conn_uri)
        if args.conn_extra is not None:
            new_conn.set_extra(args.conn_extra)
    elif args.conn_json:
        new_conn = Connection.from_json(conn_id=args.conn_id, value=args.conn_json)
        if not new_conn.conn_type:
            raise SystemExit("conn-json is invalid; must supply conn-type")
    else:
        new_conn = Connection(
            conn_id=args.conn_id,
            conn_type=args.conn_type,
            description=args.conn_description,
            host=args.conn_host,
            login=args.conn_login,
            password=args.conn_password,
            schema=args.conn_schema,
            port=args.conn_port,
        )
        if args.conn_extra is not None:
            new_conn.set_extra(args.conn_extra)

    with create_session() as session:
        if not session.scalar(select(Connection).where(Connection.conn_id == new_conn.conn_id).limit(1)):
            session.add(new_conn)
            msg = "Successfully added `conn_id`={conn_id} : {uri}"
            msg = msg.format(
                conn_id=new_conn.conn_id,
                uri=args.conn_uri
                or urlunsplit(
                    (
                        new_conn.conn_type,
                        f"{new_conn.login or ''}:{'******' if new_conn.password else ''}"
                        f"@{new_conn.host or ''}:{new_conn.port or ''}",
                        new_conn.schema or "",
                        "",
                        "",
                    )
                ),
            )
            print(msg)
        else:
            msg = f"A connection with `conn_id`={new_conn.conn_id} already exists."
            raise SystemExit(msg)


@cli_utils.action_cli
@providers_configuration_loaded
def connections_delete(args):
    """Delete connection from DB."""
    with create_session() as session:
        try:
            to_delete = session.scalars(select(Connection).where(Connection.conn_id == args.conn_id)).one()
        except exc.NoResultFound:
            raise SystemExit(f"Did not find a connection with `conn_id`={args.conn_id}")
        except exc.MultipleResultsFound:
            raise SystemExit(f"Found more than one connection with `conn_id`={args.conn_id}")
        else:
            session.delete(to_delete)
            print(f"Successfully deleted connection with `conn_id`={to_delete.conn_id}")


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def connections_import(args):
    """Import connections from a file."""
    if os.path.exists(args.file):
        _import_helper(args.file, args.overwrite)
    else:
        raise SystemExit("Missing connections file.")


def _import_helper(file_path: str, overwrite: bool) -> None:
    """Load connections from a file and save them to the DB.

    :param overwrite: Whether to skip or overwrite on collision.
    """
    connections_dict = load_connections_dict(file_path)
    with create_session() as session:
        for conn_id, conn in connections_dict.items():
            try:
                helpers.validate_key(conn_id, max_length=200)
            except Exception as e:
                print(f"Could not import connection. {e}")
                continue

            existing_conn_id = session.scalar(select(Connection.id).where(Connection.conn_id == conn_id))
            if existing_conn_id is not None:
                if not overwrite:
                    print(f"Could not import connection {conn_id}: connection already exists.")
                    continue

                # The conn_ids match, but the PK of the new entry must also be the same as the old
                conn.id = existing_conn_id

            session.merge(conn)
            session.commit()
            print(f"Imported connection {conn_id}")


@suppress_logs_and_warning
@providers_configuration_loaded
def connections_test(args) -> None:
    """Test an Airflow connection."""
    console = AirflowConsole()
    if conf.get("core", "test_connection", fallback="Disabled").lower().strip() != "enabled":
        console.print(
            "[bold yellow]\nTesting connections is disabled in Airflow configuration. "
            "Contact your deployment admin to enable it.\n"
        )
        raise SystemExit(1)

    print(f"Retrieving connection: {args.conn_id!r}")
    try:
        conn = BaseHook.get_connection(args.conn_id)
    except AirflowNotFoundException:
        console.print("[bold yellow]\nConnection not found.\n")
        raise SystemExit(1)

    print("\nTesting...")
    status, message = conn.test_connection()
    if status is True:
        console.print("[bold green]\nConnection success!\n")
    else:
        console.print(f"[bold][red]\nConnection failed![/bold]\n{message}\n")
