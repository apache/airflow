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

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import rich
from pydantic import ValidationError

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.api.datamodels.generated import (
    BulkActionOnExistence,
    BulkBodyConnectionBody,
    BulkCreateActionConnectionBody,
    ConnectionBody,
)
from airflowctl.ctl.utils.validation import split_validation_errors


def _build_connection_body(connection_id: str, details: dict[str, Any], filename: str) -> ConnectionBody:
    """Build a ``ConnectionBody``, exiting with a readable message naming the connection if it is invalid."""
    try:
        return ConnectionBody(
            connection_id=connection_id,
            conn_type=details.get("conn_type"),  # type: ignore[arg-type]
            host=details.get("host"),
            login=details.get("login"),
            password=details.get("password"),
            port=details.get("port"),
            extra=details.get("extra"),
            description=details.get("description", ""),
            **({"schema": details["schema"]} if "schema" in details else {}),
        )
    except ValidationError as e:
        missing, invalid = split_validation_errors(ConnectionBody, e, str)
        messages = []
        if missing:
            messages.append(f"missing required field(s): {', '.join(missing)}")
        messages.extend(invalid)
        raise SystemExit(f"Invalid connection {connection_id!r} in {filename}: " + "; ".join(messages))


@provide_api_client(kind=ClientKind.CLI)
def import_(args, api_client=NEW_API_CLIENT) -> None:
    """Import connections from file."""
    filepath = Path(args.file)
    current_path = Path.cwd()
    filepath = current_path / filepath if not filepath.is_absolute() else filepath
    if not filepath.exists():
        raise SystemExit(f"Missing connections file {args.file}")

    with open(filepath) as file:
        try:
            connections_json = json.loads(file.read())
        except Exception as e:
            raise SystemExit(f"Error reading connections file {args.file}: {e}")
    if not isinstance(connections_json, dict):
        raise SystemExit(
            f"Invalid connections file {args.file}: expected a JSON object mapping connection ids "
            f"to connection details, got {type(connections_json).__name__}"
        )
    for connection_id, connection_details in connections_json.items():
        if not isinstance(connection_details, dict):
            raise SystemExit(
                f"Invalid connection {connection_id!r} in {args.file}: expected a JSON object "
                f"of connection details, got {type(connection_details).__name__}"
            )
    connections_data = {k: _build_connection_body(k, v, args.file) for k, v in connections_json.items()}
    try:
        connection_create_action = BulkCreateActionConnectionBody(
            action="create",
            entities=list(connections_data.values()),
            action_on_existence=BulkActionOnExistence(args.action_on_existing_key),
        )
        response = api_client.connections.bulk(BulkBodyConnectionBody(actions=[connection_create_action]))
        if response.create.errors:
            rich.print(f"[red]Failed to import connections: {response.create.errors}[/red]")
            raise SystemExit(1)
        rich.print(f"[green]Successfully imported {response.create.success} connection(s)[/green]")
    except Exception as e:
        rich.print(f"[red]Failed to import connections: {e}[/red]")
        raise SystemExit(1)
