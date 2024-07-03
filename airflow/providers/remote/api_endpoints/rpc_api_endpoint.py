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

import functools
import json
import logging
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4

from flask import Response

from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.api_connexion.types import APIResponse

log = logging.getLogger(__name__)


@functools.lru_cache
def _initialize_method_map() -> dict[str, Callable]:
    from airflow.api_internal.endpoints.rpc_api_endpoint import initialize_method_map
    from airflow.providers.remote.models.remote_job import RemoteJob
    from airflow.providers.remote.models.remote_worker import RemoteWorker

    internal_api_functions = initialize_method_map().values()
    functions: list[Callable] = [
        # TODO Trim down functions really needed by remote worker / rebase from AIP-44
        *internal_api_functions,
        # Additional things from Remote Executor
        RemoteJob.reserve_task,
        RemoteJob.set_state,
        RemoteWorker.register_worker,
        RemoteWorker.set_state,
    ]
    return {f"{func.__module__}.{func.__qualname__}": func for func in functions}


def log_and_build_error_response(message, status):
    error_id = uuid4()
    server_message = message + f" error_id={error_id}"
    log.exception(server_message)
    client_message = message + f" The server side traceback may be identified with error_id={error_id}"
    return Response(response=client_message, status=status)


def remote_worker_api(body: dict[str, Any]) -> APIResponse:
    """Handle Remote Worker API /remote_worker/v1/rpcapi endpoint."""
    log.debug("Got request")
    json_rpc = body.get("jsonrpc")
    if json_rpc != "2.0":
        return log_and_build_error_response(message="Expected jsonrpc 2.0 request.", status=400)

    methods_map = _initialize_method_map()
    method_name = body.get("method")
    if method_name not in methods_map:
        return log_and_build_error_response(message=f"Unrecognized method: {method_name}.", status=400)

    handler = methods_map[method_name]
    params = {}
    try:
        if body.get("params"):
            params_json = body.get("params")
            params = BaseSerialization.deserialize(params_json, use_pydantic_models=True)
    except Exception:
        return log_and_build_error_response(message="Error deserializing parameters.", status=400)

    log.debug("Calling method %s\nparams: %s", method_name, params)
    try:
        # Session must be created there as it may be needed by serializer for lazy-loaded fields.
        with create_session() as session:
            output = handler(**params, session=session)
            output_json = BaseSerialization.serialize(output, use_pydantic_models=True)
            response = json.dumps(output_json) if output_json is not None else None
            return Response(response=response, headers={"Content-Type": "application/json"})
    except Exception:
        return log_and_build_error_response(message=f"Error executing method '{method_name}'.", status=500)
