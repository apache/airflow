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
"""Compatibility layer for Connexion API to Airflow v2.10 API routes."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from flask import Response, request

from airflow.exceptions import AirflowException
from airflow.providers.edge3.worker_api.auth import (
    jwt_token_authorization,
    jwt_token_authorization_rpc,
)
from airflow.providers.edge3.worker_api.datamodels import (
    EdgeJobFetched,
    JsonRpcRequest,
    PushLogsBody,
    WorkerQueuesBody,
    WorkerStateBody,
)
from airflow.providers.edge3.worker_api.routes._v2_compat import HTTPException, status
from airflow.providers.edge3.worker_api.routes.jobs import fetch, state as state_api
from airflow.providers.edge3.worker_api.routes.logs import logfile_path, push_logs
from airflow.providers.edge3.worker_api.routes.worker import register, set_state
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.session import NEW_SESSION, create_session, provide_session

if TYPE_CHECKING:
    from airflow.api_connexion.types import APIResponse
    from airflow.utils.state import TaskInstanceState


log = logging.getLogger(__name__)


def error_response(message: str, status: int):
    """Log the error and return the response as JSON object."""
    error_id = uuid4()
    server_message = f"{message} error_id={error_id}"
    log.exception(server_message)
    client_message = f"{message} The server side traceback may be identified with error_id={error_id}"
    return HTTPException(status, client_message)


def rpcapi_v2(body: dict[str, Any]) -> APIResponse:
    """Handle Edge Worker API `/edge_worker/v1/rpcapi` endpoint for Airflow 2.10."""
    # Note: Except the method map this _was_ a 100% copy of internal API module
    #       airflow.api_internal.endpoints.rpc_api_endpoint.internal_airflow_api()
    # As of rework for FastAPI in Airflow 3.0, this is updated and to be removed in the future.
    from airflow.api_internal.endpoints.rpc_api_endpoint import (
        # Note: This is just for compatibility with Airflow 2.10, not working for Airflow 3 / main as removed
        initialize_method_map,
    )

    try:
        if request.headers.get("Content-Type", "") != "application/json":
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Expected Content-Type: application/json")
        if request.headers.get("Accept", "") != "application/json":
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Expected Accept: application/json")
        auth = request.headers.get("Authorization", "")
        request_obj = JsonRpcRequest(method=body["method"], jsonrpc=body["jsonrpc"], params=body["params"])
        jwt_token_authorization_rpc(request_obj, auth)
        if request_obj.jsonrpc != "2.0":
            raise error_response("Expected jsonrpc 2.0 request.", status.HTTP_400_BAD_REQUEST)

        log.debug("Got request for %s", request_obj.method)
        methods_map = initialize_method_map()
        if request_obj.method not in methods_map:
            raise error_response(f"Unrecognized method: {request_obj.method}.", status.HTTP_400_BAD_REQUEST)

        handler = methods_map[request_obj.method]
        params = {}
        try:
            if request_obj.params:
                # Note, this is Airflow 2.10 specific, as it uses Pydantic models for serialization
                params = BaseSerialization.deserialize(request_obj.params, use_pydantic_models=True)  # type: ignore[call-arg]
        except Exception:
            raise error_response("Error deserializing parameters.", status.HTTP_400_BAD_REQUEST)

        log.debug("Calling method %s\nparams: %s", request_obj.method, params)
        try:
            # Session must be created there as it may be needed by serializer for lazy-loaded fields.
            with create_session() as session:
                output = handler(**params, session=session)
                # Note, this is Airflow 2.10 specific, as it uses Pydantic models for serialization
                output_json = BaseSerialization.serialize(output, use_pydantic_models=True)  # type: ignore[call-arg]
                log.debug(
                    "Sending response: %s", json.dumps(output_json) if output_json is not None else None
                )
        # In case of AirflowException or other selective known types, transport the exception class back to caller
        except (KeyError, AttributeError, AirflowException) as e:
            # Note, this is Airflow 2.10 specific, as it uses Pydantic models for serialization
            output_json = BaseSerialization.serialize(e, use_pydantic_models=True)  # type: ignore[call-arg]
            log.debug(
                "Sending exception response: %s", json.dumps(output_json) if output_json is not None else None
            )
        except Exception:
            raise error_response(
                f"Error executing method '{request_obj.method}'.", status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        response = json.dumps(output_json) if output_json is not None else None
        return Response(response=response, headers={"Content-Type": "application/json"})
    except HTTPException as e:
        return e.to_response()  # type: ignore[attr-defined]


def jwt_token_authorization_v2(method: str, authorization: str):
    """Proxy for v2 method path handling."""
    PREFIX = "/edge_worker/v1/"
    method_path = method[method.find(PREFIX) + len(PREFIX) :] if PREFIX in method else method
    jwt_token_authorization(method_path, authorization)


@provide_session
def register_v2(worker_name: str, body: dict[str, Any], session=NEW_SESSION) -> Any:
    """Handle Edge Worker API `/edge_worker/v1/worker/{worker_name}` endpoint for Airflow 2.10."""
    try:
        auth = request.headers.get("Authorization", "")
        jwt_token_authorization_v2(request.path, auth)
        request_obj = WorkerStateBody(
            state=body["state"], jobs_active=0, queues=body["queues"], sysinfo=body["sysinfo"]
        )
        return register(worker_name, request_obj, session).model_dump()
    except HTTPException as e:
        return e.to_response()  # type: ignore[attr-defined]


@provide_session
def set_state_v2(worker_name: str, body: dict[str, Any], session=NEW_SESSION) -> Any:
    """Handle Edge Worker API `/edge_worker/v1/worker/{worker_name}` endpoint for Airflow 2.10."""
    try:
        auth = request.headers.get("Authorization", "")
        jwt_token_authorization_v2(request.path, auth)
        request_obj = WorkerStateBody(
            state=body["state"],
            jobs_active=body["jobs_active"],
            queues=body["queues"],
            sysinfo=body["sysinfo"],
            maintenance_comments=body.get("maintenance_comments"),
        )
        return set_state(worker_name, request_obj, session).model_dump()
    except HTTPException as e:
        return e.to_response()  # type: ignore[attr-defined]


@provide_session
def job_fetch_v2(worker_name: str, body: dict[str, Any], session=NEW_SESSION) -> Any:
    """Handle Edge Worker API `/edge_worker/v1/jobs/fetch/{worker_name}` endpoint for Airflow 2.10."""
    from flask import request

    try:
        auth = request.headers.get("Authorization", "")
        jwt_token_authorization_v2(request.path, auth)
        queues = body.get("queues")
        free_concurrency = body.get("free_concurrency", 1)
        request_obj = WorkerQueuesBody(queues=queues, free_concurrency=free_concurrency)
        job: EdgeJobFetched | None = fetch(worker_name, request_obj, session)
        return job.model_dump() if job is not None else None
    except HTTPException as e:
        return e.to_response()  # type: ignore[attr-defined]


@provide_session
def job_state_v2(
    dag_id: str,
    task_id: str,
    run_id: str,
    try_number: int,
    map_index: str,  # Note: Connexion can not have negative numbers in path parameters, use string therefore
    state: TaskInstanceState,
    session=NEW_SESSION,
) -> Any:
    """Handle Edge Worker API `/jobs/state/{dag_id}/{task_id}/{run_id}/{try_number}/{map_index}/{state}` endpoint for Airflow 2.10."""
    from flask import request

    try:
        auth = request.headers.get("Authorization", "")
        jwt_token_authorization_v2(request.path, auth)
        state_api(dag_id, task_id, run_id, try_number, int(map_index), state, session)
    except HTTPException as e:
        return e.to_response()  # type: ignore[attr-defined]


def logfile_path_v2(
    dag_id: str,
    task_id: str,
    run_id: str,
    try_number: int,
    map_index: str,  # Note: Connexion can not have negative numbers in path parameters, use string therefore
) -> str:
    """Handle Edge Worker API `/edge_worker/v1/logs/logfile_path/{dag_id}/{task_id}/{run_id}/{try_number}/{map_index}` endpoint for Airflow 2.10."""
    try:
        auth = request.headers.get("Authorization", "")
        jwt_token_authorization_v2(request.path, auth)
        return logfile_path(dag_id, task_id, run_id, try_number, int(map_index))
    except HTTPException as e:
        return e.to_response()  # type: ignore[attr-defined]


def push_logs_v2(
    dag_id: str,
    task_id: str,
    run_id: str,
    try_number: int,
    map_index: str,  # Note: Connexion can not have negative numbers in path parameters, use string therefore
    body: dict[str, Any],
) -> None:
    """Handle Edge Worker API `/edge_worker/v1/logs/push/{dag_id}/{task_id}/{run_id}/{try_number}/{map_index}` endpoint for Airflow 2.10."""
    try:
        auth = request.headers.get("Authorization", "")
        jwt_token_authorization_v2(request.path, auth)
        request_obj = PushLogsBody(
            log_chunk_data=body["log_chunk_data"], log_chunk_time=body["log_chunk_time"]
        )
        with create_session() as session:
            push_logs(dag_id, task_id, run_id, try_number, int(map_index), request_obj, session)
    except HTTPException as e:
        return e.to_response()  # type: ignore[attr-defined]
