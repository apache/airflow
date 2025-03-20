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

import contextlib
import json
import os
import sys
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

import httpx
import keyring
import structlog
from platformdirs import user_config_path
from uuid6 import uuid7

from airflowctl import __version__ as version
from airflowctl.api.operations import (
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
from airflowctl.exceptions import AirflowCtlNotFoundException
from airflowctl.typing_compat import ParamSpec

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

PS = ParamSpec("PS")
RT = TypeVar("RT")


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


# Credentials for the API
class Credentials:
    """Credentials for the API."""

    api_url: str | None
    api_token: str | None
    api_environment: str

    def __init__(
        self,
        api_url: str | None = None,
        api_token: str | None = None,
        api_environment: str = "production",
    ):
        self.api_url = api_url
        self.api_token = api_token
        self.api_environment = os.getenv("AIRFLOW_CLI_ENVIRONMENT") or api_environment

    @property
    def input_cli_config_file(self) -> str:
        """Generate path for the CLI config file."""
        return f"{self.api_environment}.json"

    def save(self):
        """Save the credentials to keyring and URL to disk as a file."""
        default_config_dir = user_config_path("airflow", "Apache Software Foundation")
        if not os.path.exists(default_config_dir):
            os.makedirs(default_config_dir)
        with open(os.path.join(default_config_dir, self.input_cli_config_file), "w") as f:
            json.dump({"api_url": self.api_url}, f)
        keyring.set_password("airflow-cli", f"api_token-{self.api_environment}", self.api_token)

    def load(self) -> Credentials:
        """Load the credentials from keyring and URL from disk file."""
        default_config_dir = user_config_path("airflow", "Apache Software Foundation")
        if os.path.exists(default_config_dir):
            with open(os.path.join(default_config_dir, self.input_cli_config_file)) as f:
                credentials = json.load(f)
                self.api_url = credentials["api_url"]
                self.api_token = keyring.get_password("airflow-cli", f"api_token-{self.api_environment}")
            return self
        else:
            raise AirflowCtlNotFoundException(f"No credentials found in {default_config_dir}")


class BearerAuth(httpx.Auth):
    def __init__(self, token: str):
        self.token: str = token

    def auth_flow(self, request: httpx.Request):
        if self.token:
            request.headers["Authorization"] = "Bearer " + self.token
        yield request


class Client(httpx.Client):
    """Client for the Airflow REST API."""

    def __init__(self, *, base_url: str, token: str, **kwargs: Any):
        auth = BearerAuth(token)
        print(f"token: {token}")
        kwargs["base_url"] = f"{base_url}/public"
        pyver = f"{'.'.join(map(str, sys.version_info[:3]))}"
        super().__init__(
            auth=auth,
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


# API Client Decorator for CLI Actions
@contextlib.contextmanager
def get_client():
    """Get CLI API client."""
    cli_api_client = None
    try:
        credentials = Credentials().load()
        limits = httpx.Limits(max_keepalive_connections=1, max_connections=1)
        cli_api_client = Client(base_url=credentials.api_url, limits=limits, token=credentials.api_token)
        yield cli_api_client
    except AirflowCtlNotFoundException as e:
        raise e
    finally:
        if cli_api_client:
            cli_api_client.close()


def provide_api_client(func: Callable[PS, RT]) -> Callable[PS, RT]:
    """
    Provide a CLI API Client to the decorated function.

    CLI API Client shouldn't be passed to the function when this wrapper is used
    if the purpose is not mocking or testing.
    If you want to reuse a CLI API Client or run the function as part of
    API call, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> RT:
        if "cli_api_client" not in kwargs:
            with get_client() as cli_api_client:
                return func(*args, cli_api_client=cli_api_client, **kwargs)
        # The CLI API Client should be only passed for Mocking and Testing
        return func(*args, **kwargs)

    return wrapper


NEW_CLI_API_CLIENT: Client = cast(Client, None)
