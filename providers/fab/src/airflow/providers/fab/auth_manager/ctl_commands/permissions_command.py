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
FAB permission commands hosted by ``airflowctl``.

Each command maps 1:1 to a route in
``airflow.providers.fab.auth_manager.api_fastapi.routes.roles``:

    list -> GET /fab/v1/permissions
"""

from __future__ import annotations

from argparse import Namespace

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.ctl.console_formatting import AirflowConsole

from airflow.providers.fab.auth_manager.ctl_commands.utils import decode_response

PERMISSIONS_ENDPOINT = "/fab/v1/permissions"


@provide_api_client(kind=ClientKind.AUTH)
def list_permissions(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """GET /fab/v1/permissions."""
    response = api_client.get(
        PERMISSIONS_ENDPOINT,
        params={"limit": args.limit, "offset": args.offset, "order_by": args.order_by},
    )
    payload = decode_response(response) or {}
    # The route returns ``{"actions": [...], "total_entries": ...}`` (the
    # ``permissions`` field is aliased to ``actions`` on serialization).
    items = payload.get("actions") or payload.get("permissions") or []
    AirflowConsole().print_as(data=items, output=args.output)
