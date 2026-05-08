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
"""Shared helpers for FAB airflowctl handlers."""

from __future__ import annotations

import json
import sys
from typing import Any

import rich

_FAB_NOT_CONFIGURED_HINT = (
    "If you are running airflowctl against a remote Airflow, make sure the FAB "
    "auth manager is enabled there (apache-airflow-providers-fab installed and "
    "[core] auth_manager set to FabAuthManager). The FAB auth-manager API "
    "(/auth/fab/v1/...) is only mounted when that provider is active."
)


def decode_response(response) -> Any:
    """
    Decode a JSON response or fail with a helpful message.

    The handlers in this package call ``/auth/fab/v1/...`` endpoints that exist
    only when the FAB auth manager is configured on the remote. If those routes
    aren't mounted, the request can fall through to a non-FAB handler that
    returns a 200 with a non-JSON body (e.g. the React UI's HTML index), which
    blows up ``response.json()`` with a confusing traceback. This wrapper turns
    that into a one-line warning so the user can see what's wrong.
    """
    if response.status_code == 204 or not response.content:
        return None
    try:
        return response.json()
    except json.JSONDecodeError:
        rich.print(
            f"[red]Server returned a non-JSON response "
            f"(status {response.status_code}).[/red]\n"
            f"[yellow]{_FAB_NOT_CONFIGURED_HINT}[/yellow]"
        )
        sys.exit(1)
