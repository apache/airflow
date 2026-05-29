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
FAB user commands hosted by ``airflowctl``.

Each command maps 1:1 to a route in
``airflow.providers.fab.auth_manager.api_fastapi.routes.users``:

    list   -> GET    /fab/v1/users
    get    -> GET    /fab/v1/users/{username}
    create -> POST   /fab/v1/users
    update -> PATCH  /fab/v1/users/{username}    (with ``update_mask``)
    delete -> DELETE /fab/v1/users/{username}

Client kind is ``ClientKind.AUTH`` because that is the kind whose base URL
ends in ``/auth`` — FAB sits under the auth-manager API tree, not under
``/api/v2``.
"""

from __future__ import annotations

from argparse import Namespace

import rich
from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.ctl.console_formatting import AirflowConsole

from airflow.providers.fab.auth_manager.ctl_commands.utils import decode_response

USERS_ENDPOINT = "/fab/v1/users"


def _roles_payload(role_names: list[str] | None) -> list[dict] | None:
    if role_names is None:
        return None
    return [{"name": name} for name in role_names]


def _patchable_fields(args: Namespace) -> tuple[dict, list[str]]:
    """Build the PATCH body from supplied fields, plus the matching update_mask list."""
    body: dict = {}
    if args.email is not None:
        body["email"] = args.email
    if args.firstname is not None:
        body["first_name"] = args.firstname
    if args.lastname is not None:
        body["last_name"] = args.lastname
    if args.password is not None:
        body["password"] = args.password
    if args.role is not None:
        body["roles"] = _roles_payload(args.role)
    return body, list(body.keys())


@provide_api_client(kind=ClientKind.AUTH)
def list_users(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """GET /fab/v1/users."""
    response = api_client.get(
        USERS_ENDPOINT,
        params={"limit": args.limit, "offset": args.offset, "order_by": args.order_by},
    )
    payload = decode_response(response) or {}
    AirflowConsole().print_as(data=payload.get("users", []), output=args.output)


@provide_api_client(kind=ClientKind.AUTH)
def get_user(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """GET /fab/v1/users/{username}."""
    response = api_client.get(f"{USERS_ENDPOINT}/{args.username}")
    AirflowConsole().print_as(data=[decode_response(response)], output=args.output)


@provide_api_client(kind=ClientKind.AUTH)
def create_user(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """POST /fab/v1/users."""
    body = {
        "username": args.username,
        "email": args.email,
        "first_name": args.firstname,
        "last_name": args.lastname,
        "password": args.password,
    }
    if args.role:
        body["roles"] = _roles_payload(args.role)
    response = api_client.post(USERS_ENDPOINT, json=body)
    rich.print(f"[green]Created user {args.username!r}.[/green]")
    AirflowConsole().print_as(data=[decode_response(response)], output=args.output)


@provide_api_client(kind=ClientKind.AUTH)
def update_user(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """
    PATCH /fab/v1/users/{username}.

    Sends only the fields the user supplied. ``update_mask`` is derived from
    those fields automatically; pass ``--update-mask`` to override.
    """
    body, derived_mask = _patchable_fields(args)
    if not body:
        rich.print(
            "[yellow]No fields to update; provide at least one of "
            "--email/--firstname/--lastname/--password/--role.[/yellow]"
        )
        return
    mask = args.update_mask or ",".join(derived_mask)
    response = api_client.patch(
        f"{USERS_ENDPOINT}/{args.username}",
        json=body,
        params={"update_mask": mask},
    )
    rich.print(f"[green]Updated user {args.username!r} (fields: {mask}).[/green]")
    AirflowConsole().print_as(data=[decode_response(response)], output=args.output)


@provide_api_client(kind=ClientKind.AUTH)
def delete_user(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """DELETE /fab/v1/users/{username}."""
    api_client.delete(f"{USERS_ENDPOINT}/{args.username}")
    rich.print(f"[green]Deleted user {args.username!r}.[/green]")
