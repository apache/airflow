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
import ssl
import sys
import uuid
from functools import cache
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, TypeVar

import certifi
import httpx
import msgspec
import structlog
from pydantic import BaseModel
from tenacity import (
    before_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)
from uuid6 import uuid7

from airflow.configuration import conf
from airflow.sdk import __version__
from airflow.sdk.api.datamodels._generated import (
    API_VERSION,
    AssetEventsResponse,
    AssetResponse,
    ConnectionResponse,
    DagRunStateResponse,
    DagRunType,
    HITLDetailRequest,
    HITLDetailResponse,
    HITLUser,
    InactiveAssetsResponse,
    PrevSuccessfulDagRunResponse,
    TaskInstanceState,
    TaskStatesResponse,
    TerminalStateNonSuccess,
    TIDeferredStatePayload,
    TIEnterRunningPayload,
    TIHeartbeatInfo,
    TIRescheduleStatePayload,
    TIRetryStatePayload,
    TIRunContext,
    TISkippedDownstreamTasksStatePayload,
    TISuccessStatePayload,
    TITerminalStatePayload,
    TriggerDAGRunPayload,
    ValidationError as RemoteValidationError,
    VariablePostBody,
    VariableResponse,
    XComResponse,
    XComSequenceIndexResponse,
    XComSequenceSliceResponse,
)
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import (
    CreateHITLDetailPayload,
    DRCount,
    ErrorResponse,
    OKResponse,
    PreviousDagRunResult,
    SkipDownstreamTasks,
    TaskRescheduleStartDate,
    TICount,
    UpdateHITLDetail,
    XComCountResponse,
)

if TYPE_CHECKING:
    from datetime import datetime
    from typing import ParamSpec

    from airflow.sdk.execution_time.comms import RescheduleTask

    P = ParamSpec("P")
    T = TypeVar("T")

    # # methodtools doesn't have typestubs, so give a stub
    def lru_cache(maxsize: int | None = 128):
        def wrapper(f):
            return f

        return wrapper
else:
    from methodtools import lru_cache


@cache
def _get_fqdn(name=""):
    """
    Get fully qualified domain name from name.

    An empty argument is interpreted as meaning the local host.
    This is a patched version of socket.getfqdn() - see https://github.com/python/cpython/issues/49254
    """
    import socket

    name = name.strip()
    if not name or name == "0.0.0.0":
        name = socket.gethostname()
    try:
        addrs = socket.getaddrinfo(name, None, 0, socket.SOCK_DGRAM, 0, socket.AI_CANONNAME)
    except OSError:
        pass
    else:
        for addr in addrs:
            if addr[3]:
                name = addr[3]
                break
    return name


def get_hostname():
    """Fetch the hostname using the callable from config or use built-in FQDN as a fallback."""
    return conf.getimport("core", "hostname_callable", fallback=_get_fqdn)()


@cache
def getuser() -> str:
    """
    Get the username of the current user, or error with a nice error message if there's no current user.

    We don't want to fall back to os.getuid() because not having a username
    probably means the rest of the user environment is wrong (e.g. no $HOME).
    Explicit failure is better than silently trying to work badly.
    """
    import getpass

    try:
        return getpass.getuser()
    except KeyError:
        raise ValueError(
            "The user that Airflow is running as has no username; you must run "
            "Airflow as a full user, with a username and home directory, "
            "in order for it to function properly."
        )


log = structlog.get_logger(logger_name=__name__)

__all__ = [
    "Client",
    "ConnectionOperations",
    "ServerResponseError",
    "TaskInstanceOperations",
    "get_hostname",
    "getuser",
]


def get_json_error(response: httpx.Response):
    """Raise a ServerResponseError if we can extract error info from the error."""
    err = ServerResponseError.from_response(response)
    if err:
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
            f"Correlation-id={response.headers.get('correlation-id', None) or response.request.headers.get('correlation-id', 'no-correlation-id')}"
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

    def finish(self, id: uuid.UUID, state: TerminalStateNonSuccess, when: datetime, rendered_map_index):
        """Tell the API server that this TI has reached a terminal state."""
        if state == TaskInstanceState.SUCCESS:
            raise ValueError("Logic error. SUCCESS state should call the `succeed` function instead")
        # TODO: handle the naming better. finish sounds wrong as "even" deferred is essentially finishing.
        body = TITerminalStatePayload(
            end_date=when, state=TerminalStateNonSuccess(state), rendered_map_index=rendered_map_index
        )
        self.client.patch(f"task-instances/{id}/state", content=body.model_dump_json())

    def retry(self, id: uuid.UUID, end_date: datetime, rendered_map_index):
        """Tell the API server that this TI has failed and reached a up_for_retry state."""
        body = TIRetryStatePayload(end_date=end_date, rendered_map_index=rendered_map_index)
        self.client.patch(f"task-instances/{id}/state", content=body.model_dump_json())

    def succeed(self, id: uuid.UUID, when: datetime, task_outlets, outlet_events, rendered_map_index):
        """Tell the API server that this TI has succeeded."""
        body = TISuccessStatePayload(
            end_date=when,
            task_outlets=task_outlets,
            outlet_events=outlet_events,
            rendered_map_index=rendered_map_index,
        )
        self.client.patch(f"task-instances/{id}/state", content=body.model_dump_json())

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

    def heartbeat(self, id: uuid.UUID, pid: int):
        body = TIHeartbeatInfo(pid=pid, hostname=get_hostname())
        self.client.put(f"task-instances/{id}/heartbeat", content=body.model_dump_json())

    def skip_downstream_tasks(self, id: uuid.UUID, msg: SkipDownstreamTasks):
        """Tell the API server to skip the downstream tasks of this TI."""
        body = TISkippedDownstreamTasksStatePayload(tasks=msg.tasks)
        self.client.patch(f"task-instances/{id}/skip-downstream", content=body.model_dump_json())

    def set_rtif(self, id: uuid.UUID, body: dict[str, str]) -> OKResponse:
        """Set Rendered Task Instance Fields via the API server."""
        self.client.put(f"task-instances/{id}/rtif", json=body)
        # Any error from the server will anyway be propagated down to the supervisor,
        # so we choose to send a generic response to the supervisor over the server response to
        # decouple from the server response string
        return OKResponse(ok=True)

    def get_previous_successful_dagrun(self, id: uuid.UUID) -> PrevSuccessfulDagRunResponse:
        """
        Get the previous successful dag run for a given task instance.

        The data from it is used to get values for Task Context.
        """
        resp = self.client.get(f"task-instances/{id}/previous-successful-dagrun")
        return PrevSuccessfulDagRunResponse.model_validate_json(resp.read())

    def get_reschedule_start_date(self, id: uuid.UUID, try_number: int = 1) -> TaskRescheduleStartDate:
        """Get the start date of a task reschedule via the API server."""
        resp = self.client.get(f"task-reschedules/{id}/start_date", params={"try_number": try_number})
        return TaskRescheduleStartDate(start_date=resp.json())

    def get_count(
        self,
        dag_id: str,
        map_index: int | None = None,
        task_ids: list[str] | None = None,
        task_group_id: str | None = None,
        logical_dates: list[datetime] | None = None,
        run_ids: list[str] | None = None,
        states: list[str] | None = None,
    ) -> TICount:
        """Get count of task instances matching the given criteria."""
        params: dict[str, Any]
        params = {
            "dag_id": dag_id,
            "task_ids": task_ids,
            "task_group_id": task_group_id,
            "logical_dates": [d.isoformat() for d in logical_dates] if logical_dates is not None else None,
            "run_ids": run_ids,
            "states": states,
        }

        # Remove None values from params
        params = {k: v for k, v in params.items() if v is not None}

        if map_index is not None and map_index >= 0:
            params.update({"map_index": map_index})

        resp = self.client.get("task-instances/count", params=params)
        return TICount(count=resp.json())

    def get_task_states(
        self,
        dag_id: str,
        map_index: int | None = None,
        task_ids: list[str] | None = None,
        task_group_id: str | None = None,
        logical_dates: list[datetime] | None = None,
        run_ids: list[str] | None = None,
    ) -> TaskStatesResponse:
        """Get task states given criteria."""
        params: dict[str, Any]
        params = {
            "dag_id": dag_id,
            "task_ids": task_ids,
            "task_group_id": task_group_id,
            "logical_dates": [d.isoformat() for d in logical_dates] if logical_dates is not None else None,
            "run_ids": run_ids,
        }

        # Remove None values from params
        params = {k: v for k, v in params.items() if v is not None}

        if map_index is not None and map_index >= 0:
            params.update({"map_index": map_index})

        resp = self.client.get("task-instances/states", params=params)
        return TaskStatesResponse.model_validate_json(resp.read())

    def validate_inlets_and_outlets(self, id: uuid.UUID) -> InactiveAssetsResponse:
        """Validate whether there're inactive assets in inlets and outlets of a given task instance."""
        resp = self.client.get(f"task-instances/{id}/validate-inlets-and-outlets")
        return InactiveAssetsResponse.model_validate_json(resp.read())


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
                log.debug(
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

    def set(self, key: str, value: str | None, description: str | None = None) -> OKResponse:
        """Set an Airflow Variable via the API server."""
        body = VariablePostBody(val=value, description=description)
        self.client.put(f"variables/{key}", content=body.model_dump_json())
        # Any error from the server will anyway be propagated down to the supervisor,
        # so we choose to send a generic response to the supervisor over the server response to
        # decouple from the server response string
        return OKResponse(ok=True)

    def delete(
        self,
        key: str,
    ) -> OKResponse:
        """Delete a variable with given key via the API server."""
        self.client.delete(f"variables/{key}")
        # Any error from the server will anyway be propagated down to the supervisor,
        # so we choose to send a generic response to the supervisor over the server response to
        # decouple from the server response string
        return OKResponse(ok=True)


class XComOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def head(self, dag_id: str, run_id: str, task_id: str, key: str) -> XComCountResponse:
        """Get the number of mapped XCom values."""
        resp = self.client.head(f"xcoms/{dag_id}/{run_id}/{task_id}/{key}")

        # content_range: str | None
        if not (content_range := resp.headers["Content-Range"]) or not content_range.startswith(
            "map_indexes "
        ):
            raise RuntimeError(f"Unable to parse Content-Range header from HEAD {resp.request.url}")
        return XComCountResponse(len=int(content_range[len("map_indexes ") :]))

    def get(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        key: str,
        map_index: int | None = None,
        include_prior_dates: bool = False,
    ) -> XComResponse:
        """Get a XCom value from the API server."""
        # TODO: check if we need to use map_index as params in the uri
        # ref: https://github.com/apache/airflow/blob/v2-10-stable/airflow/api_connexion/openapi/v1.yaml#L1785C1-L1785C81
        params = {}
        if map_index is not None and map_index >= 0:
            params.update({"map_index": map_index})
        if include_prior_dates:
            params.update({"include_prior_dates": include_prior_dates})
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
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        key: str,
        value,
        map_index: int | None = None,
        mapped_length: int | None = None,
    ) -> OKResponse:
        """Set a XCom value via the API server."""
        # TODO: check if we need to use map_index as params in the uri
        # ref: https://github.com/apache/airflow/blob/v2-10-stable/airflow/api_connexion/openapi/v1.yaml#L1785C1-L1785C81
        params = {}
        if map_index is not None and map_index >= 0:
            params = {"map_index": map_index}
        if mapped_length is not None and mapped_length >= 0:
            params["mapped_length"] = mapped_length
        self.client.post(f"xcoms/{dag_id}/{run_id}/{task_id}/{key}", params=params, json=value)
        # Any error from the server will anyway be propagated down to the supervisor,
        # so we choose to send a generic response to the supervisor over the server response to
        # decouple from the server response string
        return OKResponse(ok=True)

    def delete(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        key: str,
        map_index: int | None = None,
    ) -> OKResponse:
        """Delete a XCom with given key via the API server."""
        params = {}
        if map_index is not None and map_index >= 0:
            params = {"map_index": map_index}
        self.client.delete(f"xcoms/{dag_id}/{run_id}/{task_id}/{key}", params=params)
        # Any error from the server will anyway be propagated down to the supervisor,
        # so we choose to send a generic response to the supervisor over the server response to
        # decouple from the server response string
        return OKResponse(ok=True)

    def get_sequence_item(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        key: str,
        offset: int,
    ) -> XComSequenceIndexResponse | ErrorResponse:
        try:
            resp = self.client.get(f"xcoms/{dag_id}/{run_id}/{task_id}/{key}/item/{offset}")
        except ServerResponseError as e:
            if e.response.status_code == HTTPStatus.NOT_FOUND:
                log.error(
                    "XCom not found",
                    dag_id=dag_id,
                    run_id=run_id,
                    task_id=task_id,
                    key=key,
                    offset=offset,
                    detail=e.detail,
                    status_code=e.response.status_code,
                )
                return ErrorResponse(
                    error=ErrorType.XCOM_NOT_FOUND,
                    detail={
                        "dag_id": dag_id,
                        "run_id": run_id,
                        "task_id": task_id,
                        "key": key,
                        "offset": offset,
                    },
                )
            raise
        return XComSequenceIndexResponse.model_validate_json(resp.read())

    def get_sequence_slice(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        key: str,
        start: int | None,
        stop: int | None,
        step: int | None,
        include_prior_dates: bool = False,
    ) -> XComSequenceSliceResponse:
        params = {}
        if start is not None:
            params["start"] = start
        if stop is not None:
            params["stop"] = stop
        if step is not None:
            params["step"] = step
        if include_prior_dates:
            params["include_prior_dates"] = include_prior_dates
        resp = self.client.get(f"xcoms/{dag_id}/{run_id}/{task_id}/{key}/slice", params=params)
        return XComSequenceSliceResponse.model_validate_json(resp.read())


class AssetOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def get(self, name: str | None = None, uri: str | None = None) -> AssetResponse | ErrorResponse:
        """Get Asset value from the API server."""
        if name:
            endpoint = "assets/by-name"
            params = {"name": name}
        elif uri:
            endpoint = "assets/by-uri"
            params = {"uri": uri}
        else:
            raise ValueError("Either `name` or `uri` must be provided")

        try:
            resp = self.client.get(endpoint, params=params)
        except ServerResponseError as e:
            if e.response.status_code == HTTPStatus.NOT_FOUND:
                log.error(
                    "Asset not found",
                    params=params,
                    detail=e.detail,
                    status_code=e.response.status_code,
                )
                return ErrorResponse(error=ErrorType.ASSET_NOT_FOUND, detail=params)
            raise

        return AssetResponse.model_validate_json(resp.read())


class AssetEventOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def get(
        self, name: str | None = None, uri: str | None = None, alias_name: str | None = None
    ) -> AssetEventsResponse:
        """Get Asset event from the API server."""
        if name or uri:
            resp = self.client.get("asset-events/by-asset", params={"name": name, "uri": uri})
        elif alias_name:
            resp = self.client.get("asset-events/by-asset-alias", params={"name": alias_name})
        else:
            raise ValueError("Either `name`, `uri` or `alias_name` must be provided")

        return AssetEventsResponse.model_validate_json(resp.read())


class DagRunOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def trigger(
        self,
        dag_id: str,
        run_id: str,
        conf: dict | None = None,
        logical_date: datetime | None = None,
        reset_dag_run: bool = False,
    ) -> OKResponse | ErrorResponse:
        """Trigger a Dag run via the API server."""
        body = TriggerDAGRunPayload(logical_date=logical_date, conf=conf or {}, reset_dag_run=reset_dag_run)

        try:
            self.client.post(
                f"dag-runs/{dag_id}/{run_id}", content=body.model_dump_json(exclude_defaults=True)
            )
        except ServerResponseError as e:
            if e.response.status_code == HTTPStatus.CONFLICT:
                if reset_dag_run:
                    log.info("Dag Run already exists; Resetting Dag Run.", dag_id=dag_id, run_id=run_id)
                    return self.clear(run_id=run_id, dag_id=dag_id)

                log.info("Dag Run already exists!", detail=e.detail, dag_id=dag_id, run_id=run_id)
                return ErrorResponse(error=ErrorType.DAGRUN_ALREADY_EXISTS)
            raise

        return OKResponse(ok=True)

    def clear(self, dag_id: str, run_id: str) -> OKResponse:
        """Clear a Dag run via the API server."""
        self.client.post(f"dag-runs/{dag_id}/{run_id}/clear")
        # TODO: Error handling
        return OKResponse(ok=True)

    def get_state(self, dag_id: str, run_id: str) -> DagRunStateResponse:
        """Get the state of a Dag run via the API server."""
        resp = self.client.get(f"dag-runs/{dag_id}/{run_id}/state")
        return DagRunStateResponse.model_validate_json(resp.read())

    def get_count(
        self,
        dag_id: str,
        logical_dates: list[datetime] | None = None,
        run_ids: list[str] | None = None,
        states: list[str] | None = None,
    ) -> DRCount:
        """Get count of Dag runs matching the given criteria."""
        params = {
            "dag_id": dag_id,
            "logical_dates": [d.isoformat() for d in logical_dates] if logical_dates is not None else None,
            "run_ids": run_ids,
            "states": states,
        }

        # Remove None values from params
        params = {k: v for k, v in params.items() if v is not None}

        resp = self.client.get("dag-runs/count", params=params)
        return DRCount(count=resp.json())

    def get_previous(
        self,
        dag_id: str,
        logical_date: datetime,
        state: str | None = None,
    ) -> PreviousDagRunResult:
        """Get the previous DAG run before the given logical date, optionally filtered by state."""
        params = {
            "logical_date": logical_date.isoformat(),
        }

        if state:
            params["state"] = state

        resp = self.client.get(f"dag-runs/{dag_id}/previous", params=params)
        return PreviousDagRunResult(dag_run=resp.json())


class HITLOperations:
    """
    Operations related to Human in the loop. Require Airflow 3.1+.

    :meta: private
    """

    __slots__ = ("client",)

    def __init__(self, client: Client) -> None:
        self.client = client

    def add_response(
        self,
        *,
        ti_id: uuid.UUID,
        options: list[str],
        subject: str,
        body: str | None = None,
        defaults: list[str] | None = None,
        multiple: bool = False,
        params: dict[str, dict[str, Any]] | None = None,
        assigned_users: list[HITLUser] | None = None,
    ) -> HITLDetailRequest:
        """Add a Human-in-the-loop response that waits for human response for a specific Task Instance."""
        payload = CreateHITLDetailPayload(
            ti_id=ti_id,
            options=options,
            subject=subject,
            body=body,
            defaults=defaults,
            multiple=multiple,
            params=params,
            assigned_users=assigned_users,
        )
        resp = self.client.post(
            f"/hitlDetails/{ti_id}",
            content=payload.model_dump_json(),
        )
        return HITLDetailRequest.model_validate_json(resp.read())

    def update_response(
        self,
        *,
        ti_id: uuid.UUID,
        chosen_options: list[str],
        params_input: dict[str, Any],
    ) -> HITLDetailResponse:
        """Update an existing Human-in-the-loop response."""
        payload = UpdateHITLDetail(
            ti_id=ti_id,
            chosen_options=chosen_options,
            params_input=params_input,
        )
        resp = self.client.patch(
            f"/hitlDetails/{ti_id}",
            content=payload.model_dump_json(),
        )
        return HITLDetailResponse.model_validate_json(resp.read())

    def get_detail_response(self, ti_id: uuid.UUID) -> HITLDetailResponse:
        """Get content part of a Human-in-the-loop response for a specific Task Instance."""
        resp = self.client.get(f"/hitlDetails/{ti_id}")
        return HITLDetailResponse.model_validate_json(resp.read())


class BearerAuth(httpx.Auth):
    def __init__(self, token: str):
        self.token: str = token

    def auth_flow(self, request: httpx.Request):
        if self.token:
            request.headers["Authorization"] = "Bearer " + self.token
        yield request


# This exists as an aid for debugging or local running via the `dry_run` argument to Client. It doesn't make
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
                    "consumed_asset_events": [],
                },
                "max_tries": 0,
                "should_retry": False,
            },
        )
    return httpx.Response(200, json={"text": "Hello, world!"})


# Note: Given defaults make attempts after 1, 3, 7, 15 and fails after 31seconds
API_RETRIES = conf.getint("workers", "execution_api_retries")
API_RETRY_WAIT_MIN = conf.getfloat("workers", "execution_api_retry_wait_min")
API_RETRY_WAIT_MAX = conf.getfloat("workers", "execution_api_retry_wait_max")
API_SSL_CERT_PATH = conf.get("api", "ssl_cert")
API_TIMEOUT = conf.getfloat("workers", "execution_api_timeout")


def _should_retry_api_request(exception: BaseException) -> bool:
    """Determine if an API request should be retried based on the exception type."""
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code >= 500

    return isinstance(exception, httpx.RequestError)


class Client(httpx.Client):
    @lru_cache()
    @staticmethod
    def _get_ssl_context_cached(ca_file: str, ca_path: str | None = None) -> ssl.SSLContext:
        """Cache SSL context to prevent memory growth from repeated context creation."""
        ctx = ssl.create_default_context(cafile=ca_file)
        if ca_path:
            ctx.load_verify_locations(ca_path)
        return ctx

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
            kwargs["verify"] = self._get_ssl_context_cached(certifi.where(), API_SSL_CERT_PATH)

        # Set timeout if not explicitly provided
        kwargs.setdefault("timeout", API_TIMEOUT)

        pyver = f"{'.'.join(map(str, sys.version_info[:3]))}"
        super().__init__(
            auth=auth,
            headers={
                "user-agent": f"apache-airflow-task-sdk/{__version__} (Python/{pyver})",
                "airflow-api-version": API_VERSION,
            },
            event_hooks={"response": [self._update_auth, raise_on_4xx_5xx], "request": [add_correlation_id]},
            **kwargs,
        )

    def _update_auth(self, response: httpx.Response):
        if new_token := response.headers.get("Refreshed-API-Token"):
            log.debug("Execution API issued us a refreshed Task token")
            self.auth = BearerAuth(new_token)

    @retry(
        retry=retry_if_exception(_should_retry_api_request),
        stop=stop_after_attempt(API_RETRIES),
        wait=wait_random_exponential(min=API_RETRY_WAIT_MIN, max=API_RETRY_WAIT_MAX),
        before_sleep=before_log(log, logging.WARNING),
        reraise=True,
    )
    def request(self, *args, **kwargs):
        """Implement a convenience for httpx.Client.request with a retry layer."""
        # Set content type as convenience if not already set
        if "content" in kwargs and "headers" not in kwargs:
            kwargs["headers"] = {"content-type": "application/json"}

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
    def dag_runs(self) -> DagRunOperations:
        """Operations related to DagRuns."""
        return DagRunOperations(self)

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

    @lru_cache()  # type: ignore[misc]
    @property
    def asset_events(self) -> AssetEventOperations:
        """Operations related to Asset Events."""
        return AssetEventOperations(self)

    @lru_cache()  # type: ignore[misc]
    @property
    def hitl(self):
        """Operations related to HITL Responses."""
        return HITLOperations(self)


# This is only used for parsing. ServerResponseError is raised instead
class _ErrorBody(BaseModel):
    detail: list[RemoteValidationError] | str

    def __repr__(self):
        return repr(self.detail)


class ServerResponseError(httpx.HTTPStatusError):
    def __init__(self, message: str, *, request: httpx.Request, response: httpx.Response):
        super().__init__(message, request=request, response=response)

    detail: list[RemoteValidationError] | str | dict[str, Any] | None

    def __reduce__(self) -> tuple[Any, ...]:
        # Needed because https://github.com/encode/httpx/pull/3108 isn't merged yet.
        return Exception.__new__, (type(self),) + self.args, self.__dict__

    @classmethod
    def from_response(cls, response: httpx.Response) -> ServerResponseError | None:
        if response.is_success:
            return None
        # 4xx or 5xx error?
        if not (400 <= response.status_code < 600):
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
