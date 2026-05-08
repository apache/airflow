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
"""
FAB role commands hosted by ``airflowctl``.

Each command maps 1:1 to a route in
``airflow.providers.fab.auth_manager.api_fastapi.routes.roles``:

    list   -> GET    /fab/v1/roles
    get    -> GET    /fab/v1/roles/{name}
    create -> POST   /fab/v1/roles
    update -> PATCH  /fab/v1/roles/{name}        (with ``update_mask``)
    delete -> DELETE /fab/v1/roles/{name}
"""

from __future__ import annotations

import json
import sys
from argparse import Namespace

import rich
from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.ctl.console_formatting import AirflowConsole

from airflow.providers.fab.auth_manager.ctl_commands.utils import decode_response

ROLES_ENDPOINT = "/fab/v1/roles"


def _parse_actions_json(value: str | None) -> list[dict] | None:
    """Parse a JSON list of ``{"action": {"name": ...}, "resource": {"name": ...}}`` pairs."""
    if value is None:
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        rich.print(f"[red]--actions must be a JSON array: {exc}[/red]")
        sys.exit(1)
    if not isinstance(parsed, list):
        rich.print("[red]--actions must be a JSON array.[/red]")
        sys.exit(1)
    return parsed


@provide_api_client(kind=ClientKind.AUTH)
def list_roles(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """GET /fab/v1/roles."""
    response = api_client.get(
        ROLES_ENDPOINT,
        params={"limit": args.limit, "offset": args.offset, "order_by": args.order_by},
    )
    payload = decode_response(response) or {}
    AirflowConsole().print_as(data=payload.get("roles", []), output=args.output)


@provide_api_client(kind=ClientKind.AUTH)
def get_role(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """GET /fab/v1/roles/{name}."""
    response = api_client.get(f"{ROLES_ENDPOINT}/{args.name}")
    AirflowConsole().print_as(data=[decode_response(response)], output=args.output)


@provide_api_client(kind=ClientKind.AUTH)
def create_role(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """POST /fab/v1/roles."""
    body = {"name": args.name, "actions": _parse_actions_json(args.actions) or []}
    response = api_client.post(ROLES_ENDPOINT, json=body)
    rich.print(f"[green]Created role {args.name!r}.[/green]")
    AirflowConsole().print_as(data=[decode_response(response)], output=args.output)


@provide_api_client(kind=ClientKind.AUTH)
def update_role(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """
    PATCH /fab/v1/roles/{name}.

    Sends only the fields the user supplied. ``update_mask`` is derived
    automatically; pass ``--update-mask`` to override.
    """
    body: dict = {"name": args.name}
    derived_mask: list[str] = []
    if args.new_name is not None:
        body["name"] = args.new_name
        derived_mask.append("name")
    actions = _parse_actions_json(args.actions)
    if actions is not None:
        body["actions"] = actions
        derived_mask.append("actions")
    if not derived_mask:
        rich.print("[yellow]No fields to update; provide --new-name or --actions.[/yellow]")
        return
    mask = args.update_mask or ",".join(derived_mask)
    response = api_client.patch(
        f"{ROLES_ENDPOINT}/{args.name}",
        json=body,
        params={"update_mask": mask},
    )
    rich.print(f"[green]Updated role {args.name!r} (fields: {mask}).[/green]")
    AirflowConsole().print_as(data=[decode_response(response)], output=args.output)


@provide_api_client(kind=ClientKind.AUTH)
def delete_role(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """DELETE /fab/v1/roles/{name}."""
    api_client.delete(f"{ROLES_ENDPOINT}/{args.name}")
    rich.print(f"[green]Deleted role {args.name!r}.[/green]")
