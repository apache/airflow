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
Edge worker-api health command hosted by ``airflowctl``.

Maps 1:1 to the route in
``airflow.providers.edge3.worker_api.routes.health``:

    health -> GET /edge_worker/v1/health

The edge worker-api is mounted under ``/edge_worker/v1``, which is outside the
two base URLs the ``airflowctl`` ``Client`` knows about (``/api/v2`` and
``/auth``). We therefore build an absolute URL from the existing client's
scheme + host, ignoring its ``base_url`` for this single call.
"""

from __future__ import annotations

from argparse import Namespace

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.ctl.console_formatting import AirflowConsole

EDGE_WORKER_API_PREFIX = "/edge_worker/v1"


def _absolute(api_client, path: str) -> str:
    """Build ``{scheme}://{host}{path}`` from the client's base URL."""
    base = api_client.base_url
    host = base.netloc.decode() if isinstance(base.netloc, (bytes, bytearray)) else str(base.netloc)
    scheme = base.scheme.decode() if isinstance(base.scheme, (bytes, bytearray)) else str(base.scheme)
    return f"{scheme}://{host}{path}"


@provide_api_client(kind=ClientKind.AUTH)
def health(args: Namespace, api_client=NEW_API_CLIENT) -> None:
    """GET /edge_worker/v1/health."""
    response = api_client.get(_absolute(api_client, f"{EDGE_WORKER_API_PREFIX}/health"))
    AirflowConsole().print_as(data=[response.json()], output=args.output)
