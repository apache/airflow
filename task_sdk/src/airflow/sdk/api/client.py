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

import sys
import uuid
from typing import TYPE_CHECKING, Any, TypeVar

import httpx
import msgspec
import structlog
from pydantic import BaseModel
from uuid6 import uuid7

from airflow.sdk import __version__
from airflow.sdk.api.datamodels._generated import (
    ConnectionResponse,
    TerminalTIState,
    TIEnterRunningPayload,
    TIHeartbeatInfo,
    TITerminalStatePayload,
    ValidationError as RemoteValidationError,
    VariableResponse,
    XComResponse,
)
from airflow.utils.net import get_hostname
from airflow.utils.platform import getuser

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.typing_compat import ParamSpec

    P = ParamSpec("P")
    T = TypeVar("T")

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
    "ConnectionOperations",
    "ServerResponseError",
    "TaskInstanceOperations",
]


def get_json_error(response: httpx.Response):
    """Raise a ServerResponseError if we can extract error info from the error."""
    err = ServerResponseError.from_response(response)
    if err:
        log.warning("Server error", detail=err.detail)
        raise err


def raise_on_4xx_5xx(response: httpx.Response):
    return get_json_error(response) or response.raise_for_status()


# Py 3.11+ version
def raise_on_4xx_5xx_with_note(response: httpx.Response):
    try:
        return get_json_error(response) or response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if TYPE_CHECKING:
            assert hasattr(e, "add_note")
        e.add_note(
            f"Correlation-id={response.headers.get('correlation-id', None) or response.request.headers.get('correlation-id', 'no-correlction-id')}"
        )
        raise


if hasattr(BaseException, "add_note"):
    # Py 3.11+
    raise_on_4xx_5xx = raise_on_4xx_5xx_with_note


def add_correlation_id(request: httpx.Request):
    request.headers["correlation-id"] = str(uuid7())


class TaskInstanceOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def start(self, id: uuid.UUID, pid: int, when: datetime):
        """Tell the API server that this TI has started running."""
        body = TIEnterRunningPayload(pid=pid, hostname=get_hostname(), unixname=getuser(), start_date=when)

        self.client.patch(f"task-instances/{id}/state", content=body.model_dump_json())

    def finish(self, id: uuid.UUID, state: TerminalTIState, when: datetime):
        """Tell the API server that this TI has reached a terminal state."""
        body = TITerminalStatePayload(end_date=when, state=TerminalTIState(state))

        self.client.patch(f"task-instances/{id}/state", content=body.model_dump_json())

    def heartbeat(self, id: uuid.UUID, pid: int):
        body = TIHeartbeatInfo(pid=pid, hostname=get_hostname())
        self.client.put(f"task-instances/{id}/heartbeat", content=body.model_dump_json())


class ConnectionOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def get(self, conn_id: str) -> ConnectionResponse:
        """Get a connection from the API server."""
        resp = self.client.get(f"connections/{conn_id}")
        return ConnectionResponse.model_validate_json(resp.read())


class VariableOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def get(self, key: str) -> VariableResponse:
        """Get a variable from the API server."""
        resp = self.client.get(f"variables/{key}")
        return VariableResponse.model_validate_json(resp.read())


class XComOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def get(self, dag_id: str, run_id: str, task_id: str, key: str, map_index: int = -1) -> XComResponse:
        """Get a XCom value from the API server."""
        resp = self.client.get(f"xcoms/{dag_id}/{run_id}/{task_id}/{key}", params={"map_index": map_index})
        return XComResponse.model_validate_json(resp.read())


class BearerAuth(httpx.Auth):
    def __init__(self, token: str):
        self.token: str = token

    def auth_flow(self, request: httpx.Request):
        if self.token:
            request.headers["Authorization"] = "Bearer " + self.token
        yield request


# This exists as a aid for debugging or local running via the `dry_run` argument to Client. It doesn't make
# sense for returning connections etc.
def noop_handler(request: httpx.Request) -> httpx.Response:
    log.debug("Dry-run request", method=request.method, path=request.url.path)
    return httpx.Response(200, json={"text": "Hello, world!"})


class Client(httpx.Client):
    def __init__(self, *, base_url: str | None, dry_run: bool = False, token: str, **kwargs: Any):
        if (not base_url) ^ dry_run:
            raise ValueError(f"Can only specify one of {base_url=} or {dry_run=}")
        auth = BearerAuth(token)

        if dry_run:
            # If dry run is requested, install a no op handler so that simple tasks can "heartbeat" using a
            # real client, but just don't make any HTTP requests
            kwargs["transport"] = httpx.MockTransport(noop_handler)
            kwargs["base_url"] = "dry-run://server"
        else:
            kwargs["base_url"] = base_url
        pyver = f"{'.'.join(map(str, sys.version_info[:3]))}"
        super().__init__(
            auth=auth,
            headers={"user-agent": f"apache-airflow-task-sdk/{__version__} (Python/{pyver})"},
            event_hooks={"response": [raise_on_4xx_5xx], "request": [add_correlation_id]},
            **kwargs,
        )

    # We "group" or "namespace" operations by what they operate on, rather than a flat namespace with all
    # methods on one object prefixed with the object type (`.task_instances.update` rather than
    # `task_instance_update` etc.)

    @lru_cache()  # type: ignore[misc]
    @property
    def task_instances(self) -> TaskInstanceOperations:
        """Operations related to TaskInstances."""
        return TaskInstanceOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def connections(self) -> ConnectionOperations:
        """Operations related to Connections."""
        return ConnectionOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def variables(self) -> VariableOperations:
        """Operations related to Variables."""
        return VariableOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def xcoms(self) -> XComOperations:
        """Operations related to XComs."""
        return XComOperations(self)


# This is only used for parsing. ServerResponseError is raised instead
class _ErrorBody(BaseModel):
    detail: list[RemoteValidationError] | str

    def __repr__(self):
        return repr(self.detail)


class ServerResponseError(httpx.HTTPStatusError):
    def __init__(self, message: str, *, request: httpx.Request, response: httpx.Response):
        super().__init__(message, request=request, response=response)

    detail: list[RemoteValidationError] | str | dict[str, Any] | None

    @classmethod
    def from_response(cls, response: httpx.Response) -> ServerResponseError | None:
        if response.is_success:
            return None
        # 4xx or 5xx error?
        if 400 < (response.status_code // 100) >= 600:
            return None

        if response.headers.get("content-type") != "application/json":
            return None

        detail: list[RemoteValidationError] | dict[str, Any] | None = None
        try:
            body = _ErrorBody.model_validate_json(response.read())

            if isinstance(body.detail, list):
                detail = body.detail
                msg = "Remote server returned validation error"
            else:
                msg = body.detail or "Un-parseable error"
        except Exception:
            try:
                detail = msgspec.json.decode(response.content)
            except Exception:
                # Fallback to a normal httpx error
                return None
            msg = "Server returned error"

        self = cls(msg, request=response.request, response=response)
        self.detail = detail
        return self
