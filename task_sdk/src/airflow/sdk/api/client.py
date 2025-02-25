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

import logging
import os
import sys
import uuid
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, TypeVar

import httpx
import msgspec
import structlog
from pydantic import BaseModel
from retryhttp import retry, wait_retry_after
from tenacity import before_log, wait_random_exponential
from uuid6 import uuid7

from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRuntimeCheckPayload
from airflow.sdk import __version__
from airflow.sdk.api.datamodels._generated import (
    AssetResponse,
    ConnectionResponse,
    DagRunType,
    PrevSuccessfulDagRunResponse,
    TerminalStateNonSuccess,
    TerminalTIState,
    TIDeferredStatePayload,
    TIEnterRunningPayload,
    TIHeartbeatInfo,
    TIRescheduleStatePayload,
    TIRunContext,
    TISuccessStatePayload,
    TITerminalStatePayload,
    ValidationError as RemoteValidationError,
    VariablePostBody,
    VariableResponse,
    XComResponse,
)
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import ErrorResponse, OKResponse, RuntimeCheckOnTask
from airflow.utils.net import get_hostname
from airflow.utils.platform import getuser

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.sdk.execution_time.comms import RescheduleTask
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

    def start(self, id: uuid.UUID, pid: int, when: datetime) -> TIRunContext:
        """Tell the API server that this TI has started running."""
        body = TIEnterRunningPayload(pid=pid, hostname=get_hostname(), unixname=getuser(), start_date=when)

        resp = self.client.patch(f"task-instances/{id}/run", content=body.model_dump_json())
        return TIRunContext.model_validate_json(resp.read())

    def finish(self, id: uuid.UUID, state: TerminalStateNonSuccess, when: datetime):
        """Tell the API server that this TI has reached a terminal state."""
        if state == TerminalTIState.SUCCESS:
            raise ValueError("Logic error. SUCCESS state should call the `succeed` function instead")
        # TODO: handle the naming better. finish sounds wrong as "even" deferred is essentially finishing.
        body = TITerminalStatePayload(end_date=when, state=TerminalStateNonSuccess(state))
        self.client.patch(f"task-instances/{id}/state", content=body.model_dump_json())

    def succeed(self, id: uuid.UUID, when: datetime, task_outlets, outlet_events):
        """Tell the API server that this TI has succeeded."""
        body = TISuccessStatePayload(end_date=when, task_outlets=task_outlets, outlet_events=outlet_events)
        self.client.patch(f"task-instances/{id}/state", content=body.model_dump_json())

    def heartbeat(self, id: uuid.UUID, pid: int):
        body = TIHeartbeatInfo(pid=pid, hostname=get_hostname())
        self.client.put(f"task-instances/{id}/heartbeat", content=body.model_dump_json())

    def defer(self, id: uuid.UUID, msg):
        """Tell the API server that this TI has been deferred."""
        body = TIDeferredStatePayload(**msg.model_dump(exclude_unset=True, exclude={"type"}))

        # Create a deferred state payload from msg
        self.client.patch(f"task-instances/{id}/state", content=body.model_dump_json())

    def reschedule(self, id: uuid.UUID, msg: RescheduleTask):
        """Tell the API server that this TI has been reschduled."""
        body = TIRescheduleStatePayload(**msg.model_dump(exclude_unset=True, exclude={"type"}))

        # Create a reschedule state payload from msg
        self.client.patch(f"task-instances/{id}/state", content=body.model_dump_json())

    def set_rtif(self, id: uuid.UUID, body: dict[str, str]) -> dict[str, bool]:
        """Set Rendered Task Instance Fields via the API server."""
        self.client.put(f"task-instances/{id}/rtif", json=body)
        # Any error from the server will anyway be propagated down to the supervisor,
        # so we choose to send a generic response to the supervisor over the server response to
        # decouple from the server response string
        return {"ok": True}

    def get_previous_successful_dagrun(self, id: uuid.UUID) -> PrevSuccessfulDagRunResponse:
        """
        Get the previous successful dag run for a given task instance.

        The data from it is used to get values for Task Context.
        """
        resp = self.client.get(f"task-instances/{id}/previous-successful-dagrun")
        return PrevSuccessfulDagRunResponse.model_validate_json(resp.read())

    def runtime_checks(self, id: uuid.UUID, msg: RuntimeCheckOnTask) -> OKResponse:
        body = TIRuntimeCheckPayload(**msg.model_dump(exclude_unset=True, exclude={"type"}))
        try:
            self.client.post(f"task-instances/{id}/runtime-checks", content=body.model_dump_json())
            return OKResponse(ok=True)
        except ServerResponseError as e:
            if e.response.status_code == 400:
                return OKResponse(ok=False)
            elif e.response.status_code == 409:
                # The TI isn't in the right state to perform the check, but we shouldn't fail the task for that
                return OKResponse(ok=True)
            raise


class ConnectionOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def get(self, conn_id: str) -> ConnectionResponse | ErrorResponse:
        """Get a connection from the API server."""
        try:
            resp = self.client.get(f"connections/{conn_id}")
        except ServerResponseError as e:
            if e.response.status_code == HTTPStatus.NOT_FOUND:
                log.error(
                    "Connection not found",
                    conn_id=conn_id,
                    detail=e.detail,
                    status_code=e.response.status_code,
                )
                return ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND, detail={"conn_id": conn_id})
            raise
        return ConnectionResponse.model_validate_json(resp.read())


class VariableOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def get(self, key: str) -> VariableResponse | ErrorResponse:
        """Get a variable from the API server."""
        try:
            resp = self.client.get(f"variables/{key}")
        except ServerResponseError as e:
            if e.response.status_code == HTTPStatus.NOT_FOUND:
                log.error(
                    "Variable not found",
                    key=key,
                    detail=e.detail,
                    status_code=e.response.status_code,
                )
                return ErrorResponse(error=ErrorType.VARIABLE_NOT_FOUND, detail={"key": key})
            raise
        return VariableResponse.model_validate_json(resp.read())

    def set(self, key: str, value: str | None, description: str | None = None):
        """Set an Airflow Variable via the API server."""
        body = VariablePostBody(value=value, description=description)
        self.client.put(f"variables/{key}", content=body.model_dump_json())
        # Any error from the server will anyway be propagated down to the supervisor,
        # so we choose to send a generic response to the supervisor over the server response to
        # decouple from the server response string
        return {"ok": True}


class XComOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def head(self, dag_id: str, run_id: str, task_id: str, key: str) -> int:
        """Get the number of mapped XCom values."""
        resp = self.client.head(f"xcoms/{dag_id}/{run_id}/{task_id}/{key}")

        # content_range: str | None
        if not (content_range := resp.headers["Content-Range"]) or not content_range.startswith(
            "map_indexes "
        ):
            raise RuntimeError(f"Unable to parse Content-Range header from HEAD {resp.request.url}")
        return int(content_range[len("map_indexes ") :])

    def get(
        self, dag_id: str, run_id: str, task_id: str, key: str, map_index: int | None = None
    ) -> XComResponse:
        """Get a XCom value from the API server."""
        # TODO: check if we need to use map_index as params in the uri
        # ref: https://github.com/apache/airflow/blob/v2-10-stable/airflow/api_connexion/openapi/v1.yaml#L1785C1-L1785C81
        params = {}
        if map_index is not None and map_index >= 0:
            params.update({"map_index": map_index})
        try:
            resp = self.client.get(f"xcoms/{dag_id}/{run_id}/{task_id}/{key}", params=params)
        except ServerResponseError as e:
            if e.response.status_code == HTTPStatus.NOT_FOUND:
                log.error(
                    "XCom not found",
                    dag_id=dag_id,
                    run_id=run_id,
                    task_id=task_id,
                    key=key,
                    map_index=map_index,
                    detail=e.detail,
                    status_code=e.response.status_code,
                )
                # Airflow 2.x just ignores the absence of an XCom and moves on with a return value of None
                # Hence returning with key as `key` and value as `None`, so that the message is sent back to task runner
                # and the default value of None in xcom_pull is used.
                return XComResponse(key=key, value=None)
            raise
        return XComResponse.model_validate_json(resp.read())

    def set(
        self, dag_id: str, run_id: str, task_id: str, key: str, value, map_index: int | None = None
    ) -> dict[str, bool]:
        """Set a XCom value via the API server."""
        # TODO: check if we need to use map_index as params in the uri
        # ref: https://github.com/apache/airflow/blob/v2-10-stable/airflow/api_connexion/openapi/v1.yaml#L1785C1-L1785C81
        params = {}
        if map_index is not None and map_index >= 0:
            params = {"map_index": map_index}
        self.client.post(f"xcoms/{dag_id}/{run_id}/{task_id}/{key}", params=params, json=value)
        # Any error from the server will anyway be propagated down to the supervisor,
        # so we choose to send a generic response to the supervisor over the server response to
        # decouple from the server response string
        return {"ok": True}


class AssetOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def get(self, name: str | None = None, uri: str | None = None) -> AssetResponse:
        """Get Asset value from the API server."""
        if name:
            resp = self.client.get("assets/by-name", params={"name": name})
        elif uri:
            resp = self.client.get("assets/by-uri", params={"uri": uri})
        else:
            raise ValueError("Either `name` or `uri` must be provided")

        return AssetResponse.model_validate_json(resp.read())


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
    path = request.url.path
    log.debug("Dry-run request", method=request.method, path=path)

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
                    "run_after": "2021-01-01T00:00:00Z",
                },
                "max_tries": 0,
            },
        )
    return httpx.Response(200, json={"text": "Hello, world!"})


# Config options for SDK how retries on HTTP requests should be handled
# Note: Given defaults make attempts after 1, 3, 7, 15, 31seconds, 1:03, 2:07, 3:37 and fails after 5:07min
# So far there is no other config facility in SDK we use ENV for the moment
# TODO: Consider these env variables while handling airflow confs in task sdk
API_RETRIES = int(os.getenv("AIRFLOW__WORKERS__API_RETRIES", 10))
API_RETRY_WAIT_MIN = float(os.getenv("AIRFLOW__WORKERS__API_RETRY_WAIT_MIN", 1.0))
API_RETRY_WAIT_MAX = float(os.getenv("AIRFLOW__WORKERS__API_RETRY_WAIT_MAX", 90.0))


class Client(httpx.Client):
    def __init__(self, *, base_url: str | None, dry_run: bool = False, token: str, **kwargs: Any):
        if (not base_url) ^ dry_run:
            raise ValueError(f"Can only specify one of {base_url=} or {dry_run=}")
        auth = BearerAuth(token)

        if dry_run:
            # If dry run is requested, install a no op handler so that simple tasks can "heartbeat" using a
            # real client, but just don't make any HTTP requests
            kwargs.setdefault("transport", httpx.MockTransport(noop_handler))
            kwargs.setdefault("base_url", "dry-run://server")
        else:
            kwargs["base_url"] = base_url
        pyver = f"{'.'.join(map(str, sys.version_info[:3]))}"
        super().__init__(
            auth=auth,
            headers={"user-agent": f"apache-airflow-task-sdk/{__version__} (Python/{pyver})"},
            event_hooks={"response": [raise_on_4xx_5xx], "request": [add_correlation_id]},
            **kwargs,
        )

    _default_wait = wait_random_exponential(min=API_RETRY_WAIT_MIN, max=API_RETRY_WAIT_MAX)

    @retry(
        reraise=True,
        max_attempt_number=API_RETRIES,
        wait_server_errors=_default_wait,
        wait_network_errors=_default_wait,
        wait_timeouts=_default_wait,
        wait_rate_limited=wait_retry_after(fallback=_default_wait),  # No infinite timeout on HTTP 429
        before_sleep=before_log(log, logging.WARNING),
    )
    def request(self, *args, **kwargs):
        """Implement a convenience for httpx.Client.request with a retry layer."""
        return super().request(*args, **kwargs)

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

    @lru_cache()  # type: ignore[misc]
    @property
    def assets(self) -> AssetOperations:
        """Operations related to Assets."""
        return AssetOperations(self)


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
