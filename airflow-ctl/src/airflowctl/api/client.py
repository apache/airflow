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
import enum
import json
import os
import sys
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, Literal, ParamSpec, TypeVar, cast

import httpx
import keyring
import structlog
from httpx import URL
from keyring.errors import NoKeyringError
from uuid6 import uuid7

from airflowctl import __version__ as version
from airflowctl.api.operations import (
    AssetsOperations,
    BackfillOperations,
    ConfigOperations,
    ConnectionsOperations,
    DagRunOperations,
    DagsOperations,
    JobsOperations,
    LoginOperations,
    PoolsOperations,
    ProvidersOperations,
    ServerResponseError,
    VariablesOperations,
    VersionOperations,
)
from airflowctl.exceptions import (
    AirflowCtlCredentialNotFoundException,
    AirflowCtlException,
    AirflowCtlNotFoundException,
)

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
    "provide_api_client",
    "NEW_API_CLIENT",
    "ClientKind",
    "ServerResponseError",
]

PS = ParamSpec("PS")
RT = TypeVar("RT")


class ClientKind(enum.Enum):
    """Client kind enum."""

    CLI = "cli"
    AUTH = "auth"


def add_correlation_id(request: httpx.Request):
    request.headers["correlation-id"] = str(uuid7())


def get_json_error(response: httpx.Response):
    """Raise a ServerResponseError if we can extract error info from the error."""
    err = ServerResponseError.from_response(response)
    if err:
        # This part is used in integration tests to verify the error message
        # If you are updating here don't forget to update the airflow-ctl-tests
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
        client_kind: ClientKind | None = None,
        api_environment: str = "production",
    ):
        self.api_url = api_url
        self.api_token = api_token
        self.api_environment = os.getenv("AIRFLOW_CLI_ENVIRONMENT") or api_environment
        self.client_kind = client_kind

    @property
    def input_cli_config_file(self) -> str:
        """Generate path for the CLI config file."""
        return f"{self.api_environment}.json"

    def save(self):
        """Save the credentials to keyring and URL to disk as a file."""
        default_config_dir = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
        os.makedirs(default_config_dir, exist_ok=True)
        with open(os.path.join(default_config_dir, self.input_cli_config_file), "w") as f:
            json.dump({"api_url": self.api_url}, f)

        try:
            if os.getenv("AIRFLOW_CLI_DEBUG_MODE") == "true":
                with open(
                    os.path.join(default_config_dir, f"debug_creds_{self.input_cli_config_file}"), "w"
                ) as f:
                    json.dump({f"api_token_{self.api_environment}": self.api_token}, f)
            else:
                keyring.set_password("airflowctl", f"api_token_{self.api_environment}", self.api_token)
        except NoKeyringError as e:
            log.error(e)
        except TypeError as e:
            # This happens when the token is None, which is not allowed by keyring
            if self.api_token is None and self.client_kind == ClientKind.CLI:
                raise AirflowCtlCredentialNotFoundException("No API token found. Please login first.") from e

    def load(self) -> Credentials:
        """Load the credentials from keyring and URL from disk file."""
        default_config_dir = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
        config_path = os.path.join(default_config_dir, self.input_cli_config_file)
        try:
            with open(config_path) as f:
                credentials = json.load(f)
                self.api_url = credentials["api_url"]
                if os.getenv("AIRFLOW_CLI_DEBUG_MODE") == "true":
                    debug_creds_path = os.path.join(
                        default_config_dir, f"debug_creds_{self.input_cli_config_file}"
                    )
                    with open(debug_creds_path) as df:
                        debug_credentials = json.load(df)
                        self.api_token = debug_credentials.get(f"api_token_{self.api_environment}")
                else:
                    self.api_token = keyring.get_password("airflowctl", f"api_token_{self.api_environment}")
        except FileNotFoundError:
            if self.client_kind == ClientKind.AUTH:
                # Saving the URL set from the Auth Commands if Kind is AUTH
                self.save()
            elif self.client_kind == ClientKind.CLI:
                raise AirflowCtlCredentialNotFoundException(
                    f"No credentials found in {default_config_dir} for environment {self.api_environment}."
                )
            else:
                raise AirflowCtlException(f"Unknown client kind: {self.client_kind}")

        return self


class BearerAuth(httpx.Auth):
    def __init__(self, token: str):
        self.token: str = token

    def auth_flow(self, request: httpx.Request):
        if self.token:
            request.headers["Authorization"] = "Bearer " + self.token
        yield request


class Client(httpx.Client):
    """Client for the Airflow REST API."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        kind: Literal[ClientKind.CLI, ClientKind.AUTH] = ClientKind.CLI,
        **kwargs: Any,
    ) -> None:
        auth = BearerAuth(token)
        kwargs["base_url"] = self._get_base_url(base_url=base_url, kind=kind)
        pyver = f"{'.'.join(map(str, sys.version_info[:3]))}"
        super().__init__(
            auth=auth,
            headers={"user-agent": f"apache-airflow-ctl/{version} (Python/{pyver})"},
            event_hooks={"response": [raise_on_4xx_5xx], "request": [add_correlation_id]},
            **kwargs,
        )

    def refresh_base_url(
        self, base_url: str, kind: Literal[ClientKind.AUTH, ClientKind.CLI] = ClientKind.CLI
    ):
        """Refresh the base URL of the client."""
        self.base_url = URL(self._get_base_url(base_url=base_url, kind=kind))

    @classmethod
    def _get_base_url(
        cls, base_url: str, kind: Literal[ClientKind.AUTH, ClientKind.CLI] = ClientKind.CLI
    ) -> str:
        """Get the base URL of the client."""
        if kind == ClientKind.AUTH:
            return f"{base_url}/auth"
        return f"{base_url}/api/v2"

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def login(self):
        """Operations related to authentication."""
        return LoginOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def assets(self):
        """Operations related to assets."""
        return AssetsOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def backfills(self):
        """Operations related to backfills."""
        return BackfillOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def configs(self):
        """Operations related to configs."""
        return ConfigOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def connections(self):
        """Operations related to connections."""
        return ConnectionsOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def dags(self):
        """Operations related to DAGs."""
        return DagsOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def dag_runs(self):
        """Operations related to DAG runs."""
        return DagRunOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def jobs(self):
        """Operations related to jobs."""
        return JobsOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def pools(self):
        """Operations related to pools."""
        return PoolsOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def providers(self):
        """Operations related to providers."""
        return ProvidersOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def variables(self):
        """Operations related to variables."""
        return VariablesOperations(self)

    @lru_cache()  # type: ignore[prop-decorator]
    @property
    def version(self):
        """Get the version of the server."""
        return VersionOperations(self)


# API Client Decorator for CLI Actions
@contextlib.contextmanager
def get_client(kind: Literal[ClientKind.CLI, ClientKind.AUTH] = ClientKind.CLI):
    """
    Get CLI API client.

    Don't call this method, please use @provide_api_client decorator instead.
    """
    api_client = None
    try:
        # API URL always loaded from the config file, please save with it if you are using other than ClientKind.CLI
        credentials = Credentials(client_kind=kind).load()
        api_client = Client(
            base_url=credentials.api_url or "http://localhost:8080",
            limits=httpx.Limits(max_keepalive_connections=1, max_connections=1),
            token=credentials.api_token or str(os.getenv("AIRFLOW_CLI_TOKEN", "")),
            kind=kind,
        )
        yield api_client
    except AirflowCtlNotFoundException as e:
        raise e
    finally:
        if api_client:
            api_client.close()


def provide_api_client(
    kind: Literal[ClientKind.CLI, ClientKind.AUTH] = ClientKind.CLI,
) -> Callable[[Callable[PS, RT]], Callable[PS, RT]]:
    """
    Provide a CLI API Client to the decorated function.

    CLI API Client shouldn't be passed to the function when this wrapper is used
    if the purpose is not mocking or testing.
    If you want to reuse a CLI API Client or run the function as part of
    API call, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """

    def decorator(func: Callable[PS, RT]) -> Callable[PS, RT]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> RT:
            if "api_client" not in kwargs:
                with get_client(kind=kind) as api_client:
                    return func(*args, api_client=api_client, **kwargs)
            # The CLI API Client should be only passed for Mocking and Testing
            return func(*args, **kwargs)

        return wrapper

    return decorator


NEW_API_CLIENT: Client = cast("Client", None)
