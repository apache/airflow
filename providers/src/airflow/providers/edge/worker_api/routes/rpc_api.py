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

import json
import logging
from functools import cache
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4

from itsdangerous import BadSignature
from jwt import (
    ExpiredSignatureError,
    ImmatureSignatureError,
    InvalidAudienceError,
    InvalidIssuedAtError,
    InvalidSignatureError,
)

from airflow.api_internal.endpoints.rpc_api_endpoint import (
    initialize_method_map,
)
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.edge.worker_api.datamodels import JsonRpcRequest
from airflow.providers.edge.worker_api.routes._v2_compat import (
    AirflowRouter,
    Depends,
    Header,
    HTTPException,
    create_openapi_http_exception_doc,
    status,
)
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.jwt_signer import JWTSigner
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.api_connexion.types import APIResponse

log = logging.getLogger(__name__)
rpc_api_router = AirflowRouter(tags=["JSONRPC"])


@cache
def _initialize_method_map() -> dict[str, Callable]:
    from airflow.providers.edge.models.edge_job import EdgeJob
    from airflow.providers.edge.models.edge_logs import EdgeLogs
    from airflow.providers.edge.models.edge_worker import EdgeWorker

    internal_api_functions = initialize_method_map().values()
    functions: list[Callable] = [
        *internal_api_functions,
        # Additional things from EdgeExecutor
        EdgeJob.reserve_task,
        EdgeJob.set_state,
        EdgeLogs.push_logs,
        EdgeWorker.register_worker,
        EdgeWorker.set_state,
    ]
    return {f"{func.__module__}.{func.__qualname__}": func for func in functions}


@cache
def _jwt_signer() -> JWTSigner:
    clock_grace = conf.getint("core", "internal_api_clock_grace", fallback=30)
    return JWTSigner(
        secret_key=conf.get("core", "internal_api_secret_key"),
        expiration_time_in_seconds=clock_grace,
        leeway_in_seconds=clock_grace,
        audience="api",
    )


def error_response(message: str, status: int):
    """Log the error and return the response as JSON object."""
    error_id = uuid4()
    server_message = f"{message} error_id={error_id}"
    log.exception(server_message)
    client_message = f"{message} The server side traceback may be identified with error_id={error_id}"
    return HTTPException(status, client_message)


def json_request_headers(content_type: str = Header(), accept: str = Header()):
    """Check if the request headers are correct."""
    if content_type != "application/json":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Expected Content-Type: application/json")
    if accept != "application/json":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Expected Accept: application/json")


def jwt_token_authorization(body: JsonRpcRequest, authorization: str = Header()):
    """Check if the JWT token is correct."""
    try:
        payload = _jwt_signer().verify_token(authorization)
        signed_method = payload.get("method")
        if not signed_method or signed_method != body.method:
            raise BadSignature("Invalid method in token authorization.")
    except BadSignature:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "Bad Signature. Please use only the tokens provided by the API."
        )
    except InvalidAudienceError:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Invalid audience for the request")
    except InvalidSignatureError:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "The signature of the request was wrong")
    except ImmatureSignatureError:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "The signature of the request was sent from the future"
        )
    except ExpiredSignatureError:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "The signature of the request has expired. Make sure that all components "
            "in your system have synchronized clocks.",
        )
    except InvalidIssuedAtError:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "The request was issues in the future. Make sure that all components "
            "in your system have synchronized clocks.",
        )
    except Exception:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Unable to authenticate API via token.")


def json_rpc_version(body: JsonRpcRequest):
    """Check if the JSON RPC Request version is correct."""
    if body.jsonrpc != "2.0":
        raise error_response("Expected jsonrpc 2.0 request.", status.HTTP_400_BAD_REQUEST)


@rpc_api_router.post(
    "/rpcapi",
    dependencies=[Depends(json_request_headers), Depends(jwt_token_authorization), Depends(json_rpc_version)],
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
        ]
    ),
)
def rpcapi(body: JsonRpcRequest) -> dict | None:
    """Handle Edge Worker API calls as JSON-RPC."""
    log.debug("Got request for %s", body.method)
    methods_map = _initialize_method_map()
    if body.method not in methods_map:
        raise error_response(f"Unrecognized method: {body.method}.", status.HTTP_400_BAD_REQUEST)

    handler = methods_map[body.method]
    params = {}
    try:
        if body.params:
            params = BaseSerialization.deserialize(body.params, use_pydantic_models=True)
    except Exception:
        raise error_response("Error deserializing parameters.", status.HTTP_400_BAD_REQUEST)

    log.debug("Calling method %s\nparams: %s", body.method, params)
    try:
        # Session must be created there as it may be needed by serializer for lazy-loaded fields.
        with create_session() as session:
            output = handler(**params, session=session)
            output_json = BaseSerialization.serialize(output, use_pydantic_models=True)
            log.debug("Sending response: %s", json.dumps(output_json) if output_json is not None else None)
            return output_json
    # In case of AirflowException or other selective known types, transport the exception class back to caller
    except (KeyError, AttributeError, AirflowException) as e:
        exception_json = BaseSerialization.serialize(e, use_pydantic_models=True)
        log.debug(
            "Sending exception response: %s", json.dumps(output_json) if output_json is not None else None
        )
        return exception_json
    except Exception:
        raise error_response(
            f"Error executing method '{body.method}'.", status.HTTP_500_INTERNAL_SERVER_ERROR
        )


def edge_worker_api_v2(body: dict[str, Any]) -> APIResponse:
    """Handle Edge Worker API `/edge_worker/v1/rpcapi` endpoint for Airflow 2.10."""
    # Note: Except the method map this _was_ a 100% copy of internal API module
    #       airflow.api_internal.endpoints.rpc_api_endpoint.internal_airflow_api()
    # As of rework for FastAPI in Airflow 3.0, this is updated and to be removed in future.
    from flask import Response, request

    try:
        json_request_headers(
            content_type=request.headers.get("Content-Type", ""), accept=request.headers.get("Accept", "")
        )

        auth = request.headers.get("Authorization", "")
        json_rpc = body.get("jsonrpc", "")
        method_name = body.get("method", "")
        request_obj = JsonRpcRequest(method=method_name, jsonrpc=json_rpc, params=body.get("params"))
        jwt_token_authorization(request_obj, auth)

        json_rpc_version(request_obj)

        output_json = rpcapi(request_obj)
        response = json.dumps(output_json) if output_json is not None else None
        return Response(response=response, headers={"Content-Type": "application/json"})
    except HTTPException as e:
        return e.to_response()  # type: ignore[attr-defined]
