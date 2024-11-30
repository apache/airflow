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
from functools import cache
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4

from airflow.exceptions import AirflowException
from airflow.providers.edge.worker_api.auth import jwt_token_authorization, jwt_token_authorization_rpc
from airflow.providers.edge.worker_api.datamodels import JsonRpcRequest, WorkerStateBody
from airflow.providers.edge.worker_api.routes._v2_compat import HTTPException, status
from airflow.providers.edge.worker_api.routes.worker import register, set_state
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.session import NEW_SESSION, create_session, provide_session

if TYPE_CHECKING:
    from airflow.api_connexion.types import APIResponse


log = logging.getLogger(__name__)


@cache
def _initialize_method_map() -> dict[str, Callable]:
    # Note: This is a copy of the (removed) AIP-44 implementation from
    #       airflow/api_internal/endpoints/rpc_api_endpoint.py
    #       for compatibility with Airflow 2.10-line.
    #       Methods are potentially not existing more on main branch for Airflow 3.
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.cli.commands.task_command import _get_ti_db_access
    from airflow.dag_processing.manager import DagFileProcessorManager
    from airflow.dag_processing.processor import DagFileProcessor

    # Airflow 2.10 compatibility
    from airflow.datasets import expand_alias_to_datasets  # type: ignore[attr-defined]
    from airflow.datasets.manager import DatasetManager  # type: ignore[attr-defined]
    from airflow.jobs.job import Job, most_recent_job
    from airflow.models import Trigger, Variable, XCom
    from airflow.models.dag import DAG, DagModel
    from airflow.models.dagcode import DagCode
    from airflow.models.dagrun import DagRun
    from airflow.models.dagwarning import DagWarning
    from airflow.models.renderedtifields import RenderedTaskInstanceFields
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.models.skipmixin import SkipMixin
    from airflow.models.taskinstance import (
        TaskInstance,
        _add_log,
        _defer_task,
        _get_template_context,
        _handle_failure,
        _handle_reschedule,
        _record_task_map_for_downstreams,
        _update_rtif,
        _xcom_pull,
    )
    from airflow.models.xcom_arg import _get_task_map_length
    from airflow.providers.edge.models.edge_job import EdgeJob
    from airflow.providers.edge.models.edge_logs import EdgeLogs
    from airflow.providers.edge.models.edge_worker import EdgeWorker
    from airflow.secrets.metastore import MetastoreBackend
    from airflow.sensors.base import _orig_start_date
    from airflow.utils.cli_action_loggers import _default_action_log_internal
    from airflow.utils.log.file_task_handler import FileTaskHandler

    functions: list[Callable] = [
        _default_action_log_internal,
        _defer_task,
        _get_template_context,
        _get_ti_db_access,
        _get_task_map_length,
        _update_rtif,
        _orig_start_date,
        _handle_failure,
        _handle_reschedule,
        _add_log,
        _xcom_pull,
        _record_task_map_for_downstreams,
        trigger_dag,
        DagCode.remove_deleted_code,
        DagModel.deactivate_deleted_dags,
        DagModel.get_paused_dag_ids,
        DagModel.get_current,
        DagFileProcessor._execute_task_callbacks,
        DagFileProcessor.execute_callbacks,
        DagFileProcessor.execute_callbacks_without_dag,
        # Airflow 2.10 compatibility
        DagFileProcessor.manage_slas,  # type: ignore[attr-defined]
        DagFileProcessor.save_dag_to_db,
        DagFileProcessor.update_import_errors,
        DagFileProcessor._validate_task_pools_and_update_dag_warnings,
        DagFileProcessorManager._fetch_callbacks,
        DagFileProcessorManager._get_priority_filelocs,
        DagFileProcessorManager.clear_nonexistent_import_errors,
        DagFileProcessorManager.deactivate_stale_dags,
        DagWarning.purge_inactive_dag_warnings,
        expand_alias_to_datasets,
        DatasetManager.register_dataset_change,
        FileTaskHandler._render_filename_db_access,
        Job._add_to_db,
        Job._fetch_from_db,
        Job._kill,
        Job._update_heartbeat,
        Job._update_in_db,
        most_recent_job,
        # Airflow 2.10 compatibility
        MetastoreBackend._fetch_connection,  # type: ignore[attr-defined]
        MetastoreBackend._fetch_variable,  # type: ignore[attr-defined]
        XCom.get_value,
        XCom.get_one,
        # XCom.get_many, # Not supported because it returns query
        XCom.clear,
        XCom.set,
        Variable._set,
        Variable._update,
        Variable._delete,
        DAG.fetch_callback,
        DAG.fetch_dagrun,
        DagRun.fetch_task_instances,
        DagRun.get_previous_dagrun,
        DagRun.get_previous_scheduled_dagrun,
        DagRun.get_task_instances,
        DagRun.fetch_task_instance,
        DagRun._get_log_template,
        RenderedTaskInstanceFields._update_runtime_evaluated_template_fields,
        SerializedDagModel.get_serialized_dag,
        SerializedDagModel.remove_deleted_dags,
        SkipMixin._skip,
        SkipMixin._skip_all_except,
        TaskInstance._check_and_change_state_before_execution,
        TaskInstance.get_task_instance,
        TaskInstance._get_dagrun,
        TaskInstance._set_state,
        TaskInstance.save_to_db,
        TaskInstance._clear_xcom_data,
        Trigger.from_object,
        Trigger.bulk_fetch,
        Trigger.clean_unused,
        Trigger.submit_event,
        Trigger.submit_failure,
        Trigger.ids_for_triggerer,
        Trigger.assign_unassigned,
        # Additional things from EdgeExecutor
        # These are removed in follow-up PRs as being in transition to FastAPI
        EdgeJob.reserve_task,
        EdgeJob.set_state,
        EdgeLogs.push_logs,
        EdgeWorker.register_worker,
        EdgeWorker.set_state,
    ]
    return {f"{func.__module__}.{func.__qualname__}": func for func in functions}


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
    from flask import Response, request

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
        methods_map = _initialize_method_map()
        if request_obj.method not in methods_map:
            raise error_response(f"Unrecognized method: {request_obj.method}.", status.HTTP_400_BAD_REQUEST)

        handler = methods_map[request_obj.method]
        params = {}
        try:
            if request_obj.params:
                params = BaseSerialization.deserialize(request_obj.params, use_pydantic_models=True)
        except Exception:
            raise error_response("Error deserializing parameters.", status.HTTP_400_BAD_REQUEST)

        log.debug("Calling method %s\nparams: %s", request_obj.method, params)
        try:
            # Session must be created there as it may be needed by serializer for lazy-loaded fields.
            with create_session() as session:
                output = handler(**params, session=session)
                output_json = BaseSerialization.serialize(output, use_pydantic_models=True)
                log.debug(
                    "Sending response: %s", json.dumps(output_json) if output_json is not None else None
                )
        # In case of AirflowException or other selective known types, transport the exception class back to caller
        except (KeyError, AttributeError, AirflowException) as e:
            output_json = BaseSerialization.serialize(e, use_pydantic_models=True)
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


@provide_session
def register_v2(worker_name: str, body: dict[str, Any], session=NEW_SESSION) -> Any:
    """Handle Edge Worker API `/edge_worker/v1/worker/{worker_name}` endpoint for Airflow 2.10."""
    from flask import request

    try:
        auth = request.headers.get("Authorization", "")
        jwt_token_authorization(request.path, auth)
        request_obj = WorkerStateBody(
            state=body["state"], jobs_active=0, queues=body["queues"], sysinfo=body["sysinfo"]
        )
        return register(worker_name, request_obj, session)
    except HTTPException as e:
        return e.to_response()  # type: ignore[attr-defined]


@provide_session
def set_state_v2(worker_name: str, body: dict[str, Any], session=NEW_SESSION) -> Any:
    """Handle Edge Worker API `/edge_worker/v1/worker/{worker_name}` endpoint for Airflow 2.10."""
    from flask import request

    try:
        auth = request.headers.get("Authorization", "")
        jwt_token_authorization(request.path, auth)
        request_obj = WorkerStateBody(
            state=body["state"],
            jobs_active=body["jobs_active"],
            queues=body["queues"],
            sysinfo=body["sysinfo"],
        )
        return set_state(worker_name, request_obj, session)
    except HTTPException as e:
        return e.to_response()  # type: ignore[attr-defined]
