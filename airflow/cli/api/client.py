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

import base64
import json
import os
import sys
from typing import TYPE_CHECKING, Any

import httpx
import rich
import structlog
from uuid6 import uuid7

from airflow.cli.api.operations import (
    AssetsOperations,
    BackfillsOperations,
    ConfigOperations,
    ConnectionsOperations,
    DagOperations,
    DagRunOperations,
    JobsOperations,
    PoolsOperations,
    ProvidersOperations,
    ServerResponseError,
    VariablesOperations,
    VersionOperations,
)
from airflow.utils.types import DagRunType
from airflow.version import version

if TYPE_CHECKING:
    # # methodtools doesn't have typestubs, so give a stub
    def lru_cache(maxsize: int | None = 128):
        def wrapper(f):
            return f

        return wrapper
else:
    from methodtools import lru_cache

log = structlog.get_logger(logger_name=__name__)

__all__ = [
    "Client",
    "Credentials",
]


def add_correlation_id(request: httpx.Request):
    request.headers["correlation-id"] = str(uuid7())


def get_json_error(response: httpx.Response):
    """Raise a ServerResponseError if we can extract error info from the error."""
    err = ServerResponseError.from_response(response)
    if err:
        log.warning("Server error ", extra=dict(err.response.json()))
        raise err


def raise_on_4xx_5xx(response: httpx.Response):
    return get_json_error(response) or response.raise_for_status()


def noop_handler(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    log.debug("Dry-run request", method=request.method, path=path)
    # TODO change for test
    if path.startswith("/task-instances/") and path.endswith("/run"):
        # Return a fake context
        return httpx.Response(
            200,
            json={
                "dag_run": {
                    "dag_id": "test_dag",
                    "run_id": "test_run",
                    "logical_date": "2021-01-01T00:00:00Z",
                    "start_date": "2021-01-01T00:00:00Z",
                    "run_type": DagRunType.MANUAL,
                },
            },
        )
    return httpx.Response(200, json={"text": "Hello, world!"})


# Credentials for the API
class Credentials:
    """Credentials for the API."""

    api_url: str
    api_token: str

    def __init__(self, api_url: str, api_token: str = "NO_TOKEN"):
        self.api_url = api_url
        self.api_token = api_token

    def save(self):
        """Save the credentials."""
        default_config_dir = os.path.expanduser("~/.config/airflow")
        print(f"Saving credentials to {default_config_dir}")
        if not os.path.exists(default_config_dir):
            os.makedirs(default_config_dir)
        with open(os.path.join(default_config_dir, "credentials.json"), "w") as f:
            json.dump(
                {"api_url": self.api_url, "api_token": base64.b64encode(self.api_token.encode()).decode()}, f
            )

    @classmethod
    def load(cls) -> Credentials:
        """Load the credentials."""
        default_config_dir = os.path.expanduser("~/.config/airflow")
        if os.path.exists(default_config_dir):
            with open(os.path.join(default_config_dir, "credentials.json")) as f:
                credentials = json.load(f)
            return cls(
                api_url=credentials["api_url"],
                api_token=base64.b64decode(credentials["api_token"]).decode(),
            )
        else:
            rich.print("[red]No credentials found.")
            rich.print("[green]Please run: [blue]airflow auth login")
            sys.exit(1)


class Client(httpx.Client):
    """Client for the Airflow REST API."""

    def __init__(self, *, base_url: str | None, dry_run: bool = False, **kwargs: Any):
        if (not base_url) ^ dry_run:
            raise ValueError(f"Can only specify one of {base_url=} or {dry_run=}")
        if dry_run:
            # If dry run is requested, install a no op handler so that simple tasks can "heartbeat" using a
            # real client, but just don't make any HTTP requests
            kwargs["transport"] = httpx.MockTransport(noop_handler)
            kwargs["base_url"] = "dry-run://server"
        else:
            kwargs["base_url"] = f"{base_url}/public"
        pyver = f"{'.'.join(map(str, sys.version_info[:3]))}"
        # TODO add auth using token after sign in integrated
        super().__init__(
            auth=None,
            headers={"user-agent": f"apache-airflow-cli/{version} (Python/{pyver})"},
            event_hooks={"response": [raise_on_4xx_5xx], "request": [add_correlation_id]},
            **kwargs,
        )

    @lru_cache()  # type: ignore[misc]
    @property
    def assets(self):
        """Operations related to assets."""
        return AssetsOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def backfills(self):
        """Operations related to backfills."""
        return BackfillsOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def configs(self):
        """Operations related to configs."""
        return ConfigOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def connections(self):
        """Operations related to connections."""
        return ConnectionsOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def dags(self):
        """Operations related to DAGs."""
        return DagOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def dag_runs(self):
        """Operations related to DAG runs."""
        return DagRunOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def jobs(self):
        """Operations related to jobs."""
        return JobsOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def pools(self):
        """Operations related to pools."""
        return PoolsOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def providers(self):
        """Operations related to providers."""
        return ProvidersOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def variables(self):
        """Operations related to variables."""
        return VariablesOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def version(self):
        """Get the version of the server."""
        return VersionOperations(self)
