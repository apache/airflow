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

import rich

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.api.datamodels.generated import (
    BulkActionOnExistence,
    BulkBodyConnectionBody,
    BulkCreateActionConnectionBody,
    ConnectionBody,
)


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
    try:
        connections_data = {
            k: ConnectionBody(
                connection_id=k,
                conn_type=v.get("conn_type"),
                host=v.get("host"),
                login=v.get("login"),
                password=v.get("password"),
                port=v.get("port"),
                extra=v.get("extra"),
                description=v.get("description", ""),
                **({"schema": v["schema"]} if "schema" in v else {}),
            )
            for k, v in connections_json.items()
        }
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
