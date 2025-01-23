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

import json
import os
import warnings
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

from airflow.cli.api.cli_api_client import NEW_CLI_API_CLIENT, provide_cli_api_client
from airflow.cli.api.datamodels._generated import (
    BulkAction,
    BulkActionOnExistence,
    ConnectionBody,
    ConnectionBulkBody,
    ConnectionBulkCreateAction,
    ConnectionResponse,
    ConnectionTestResponse,
)
from airflow.cli.simple_table import AirflowConsole
from airflow.cli.utils import is_stdout, print_export_output
from airflow.configuration import conf
from airflow.models import Connection
from airflow.providers_manager import ProvidersManager
from airflow.secrets.local_filesystem import load_connections_dict
from airflow.utils import cli as cli_utils, helpers, yaml
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

if TYPE_CHECKING:
    from airflow.cli.api.client import Client


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
@provide_cli_api_client
def connections_get(args, cli_api_client=NEW_CLI_API_CLIENT):
    """Get a connection."""
    connection = cli_api_client.connections.get(conn_id=args.conn_id)
    AirflowConsole().print_as(
        data=[_response_to_connection(connection)],
        output=args.output,
        mapper=_connection_mapper,
    )


@suppress_logs_and_warning
@providers_configuration_loaded
@provide_cli_api_client
def connections_list(args, cli_api_client=NEW_CLI_API_CLIENT):
    """List all connections at the command line."""
    cli_api_client.connections.exit_in_error = True
    AirflowConsole().print_as(
        data=[
            _response_to_connection(connection)
            for connection in cli_api_client.connections.list().connections
        ],
        output=args.output,
        mapper=_connection_mapper,
    )


def _connection_to_dict(conn: Connection) -> dict:
    return {
        "conn_type": conn.conn_type,
        "description": conn.description,
        "login": conn.login,
        "password": conn.password,
        "host": conn.host,
        "port": conn.port,
        "schema": conn.schema,
        "extra": conn.extra,
    }


@provide_cli_api_client
def create_default_connections(args, cli_api_client=NEW_CLI_API_CLIENT):
    """Create default connections."""
    cli_api_client.connections.create_defaults()
    print("Default connections have been created.")


def _response_to_connection(response: ConnectionResponse) -> Connection:
    # TODO: Return is_encrypted from API to properly convert to Connection, otherwise it will be None
    return Connection(
        conn_id=response.connection_id,
        conn_type=response.conn_type,
        description=response.description,
        host=response.host,
        login=response.login,
        password=response.password,
        schema=response.schema_,
        port=response.port,
        extra=response.extra,
    )


def _connection_to_request(conn: Connection) -> ConnectionBody:
    return ConnectionBody(
        connection_id=conn.conn_id,
        conn_type=conn.conn_type,
        description=conn.description,
        host=conn.host,
        login=conn.login,
        password=conn.password,
        schema_=conn.schema,
        port=conn.port,
        extra=conn.extra,
    )


def _format_connections(
    connection_responses: list[ConnectionResponse], file_format: str, serialization_format: str
) -> str:
    conns: list[Connection] = [_response_to_connection(conn) for conn in connection_responses]

    if serialization_format == "json":

        def serializer_func(x: Connection) -> str:
            return json.dumps(_connection_to_dict(x))

    elif serialization_format == "uri":
        serializer_func = Connection.get_uri  # type: ignore
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


def _valid_uri(uri: str) -> bool:
    """Check if a URI is valid, by checking if scheme (conn_type) provided."""
    return urlsplit(uri).scheme != ""


@cache
def _get_connection_types() -> list[str]:
    """Return connection types available."""
    _connection_types = []
    providers_manager = ProvidersManager()
    for connection_type, provider_info in providers_manager.hooks.items():
        if provider_info:
            _connection_types.append(connection_type)
    return _connection_types


@providers_configuration_loaded
@provide_cli_api_client
def connections_export(args, cli_api_client=NEW_CLI_API_CLIENT):
    """Export all connections to a file."""
    file_formats = [".yaml", ".json", ".env"]
    if args.format:
        warnings.warn(
            "Option `--format` is deprecated. Use `--file-format` instead.", DeprecationWarning, stacklevel=3
        )
    if args.format and args.file_format:
        raise SystemExit("Option `--format` is deprecated.  Use `--file-format` instead.")
    default_format = ".json"
    provided_file_format = None
    if args.format or args.file_format:
        provided_file_format = f".{(args.format or args.file_format).lower()}"

    with args.file as f:
        if is_stdout(f):
            filetype = provided_file_format or default_format
        elif provided_file_format:
            filetype = provided_file_format
        else:
            filetype = Path(args.file.name).suffix.lower()
            if filetype not in file_formats:
                raise SystemExit(
                    f"Unsupported file format. The file must have the extension {', '.join(file_formats)}."
                )

        if args.serialization_format and filetype != ".env":
            raise SystemExit("Option `--serialization-format` may only be used with file type `env`.")

        connection_response = cli_api_client.connections.list()
        msg = _format_connections(
            connection_responses=connection_response.connections,
            file_format=filetype,
            serialization_format=args.serialization_format or "uri",
        )

        f.write(msg)

    print_export_output("Connections", connection_response.connections, f)


alternative_conn_specs = ["conn_type", "conn_host", "conn_login", "conn_password", "conn_schema", "conn_port"]


@cli_utils.action_cli
@providers_configuration_loaded
@provide_cli_api_client
def connections_add(args, cli_api_client=NEW_CLI_API_CLIENT):
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
        warnings.warn(
            f"The type provided to --conn-type is invalid: {args.conn_type}", UserWarning, stacklevel=4
        )
        warnings.warn(
            f"Supported --conn-types are:{_get_connection_types()}."
            "Hence overriding the conn-type with generic",
            UserWarning,
            stacklevel=4,
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

    cli_api_client.connections.create(connection=_connection_to_request(new_conn))
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


@cli_utils.action_cli
@providers_configuration_loaded
@provide_cli_api_client
def connections_delete(args, cli_api_client=NEW_CLI_API_CLIENT):
    """Delete connection from DB."""
    cli_api_client.connections.delete(conn_id=args.conn_id)
    print(f"Successfully deleted connection with `conn_id`={args.conn_id}")


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
@provide_cli_api_client
def connections_import(args, cli_api_client=NEW_CLI_API_CLIENT):
    """Import connections from a file."""
    if os.path.exists(args.file):
        _import_helper(file_path=args.file, overwrite=args.overwrite, cli_api_client=cli_api_client)
    else:
        raise SystemExit("Missing connections file.")


def _import_helper(file_path: str, overwrite: bool, cli_api_client: Client) -> None:
    """
    Load connections from a file and save them to the DB.

    :param overwrite: Whether to skip or overwrite on collision.
    """
    # TODO: Update Bulk Import endpoint to conform overwrite option and include into operation
    connections = load_connections_dict(file_path)
    connections_to_create = []
    for connection_id, connection in connections.items():
        connections_to_create.append(
            ConnectionBody(
                connection_id=connection_id,
                conn_type=connection.conn_type,
                description=connection.description,
                host=connection.host,
                login=connection.login,
                password=connection.password,
                schema_=connection.schema,
                port=connection.port,
                extra=connection.extra,
            )
        )

    cli_api_client.connections.bulk(
        connections=ConnectionBulkBody(
            actions=[
                ConnectionBulkCreateAction(
                    action=BulkAction.CREATE,
                    connections=connections_to_create,
                    action_on_existence=BulkActionOnExistence.FAIL
                    if not overwrite
                    else BulkActionOnExistence.OVERWRITE,
                )
            ]
        )
    )
    for conn_id in connections:
        print(f"Imported connection {conn_id}")


@suppress_logs_and_warning
@providers_configuration_loaded
@provide_cli_api_client
def connections_test(args, cli_api_client=NEW_CLI_API_CLIENT) -> None:
    """Test an Airflow connection."""
    console = AirflowConsole()
    if conf.get("core", "test_connection", fallback="Disabled").lower().strip() != "enabled":
        console.print(
            "[bold yellow]\nTesting connections is disabled in Airflow configuration. "
            "Contact your deployment admin to enable it.\n"
        )
        raise SystemExit(1)

    print("\nTesting...")
    connection_test_response: ConnectionTestResponse = cli_api_client.connections.test(
        ConnectionBody(
            connection_id=args.conn_id,
            conn_type=args.conn_type,
        )
    )

    status, message = (
        connection_test_response.model_dump().get("status"),
        connection_test_response.model_dump().get("message"),
    )
    if status is True:
        console.print(f"[bold green]\nConnection success! {connection_test_response}\n")
    else:
        console.print(f"[bold][red]\nConnection failed![/bold]\n{message}\n")
