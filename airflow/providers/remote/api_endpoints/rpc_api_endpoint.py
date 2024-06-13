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

from airflow.jobs.job import Job, most_recent_job
from airflow.sensors.base import _orig_start_date
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.api_connexion.types import APIResponse

log = logging.getLogger(__name__)


@functools.lru_cache
def _initialize_map() -> dict[str, Callable]:
    # TODO Trim down functions really needed by remote worker / rebase from AIP-44
    from airflow.cli.commands.task_command import _get_ti_db_access
    from airflow.dag_processing.manager import DagFileProcessorManager
    from airflow.dag_processing.processor import DagFileProcessor
    from airflow.datasets.manager import DatasetManager
    from airflow.models import Trigger, Variable, XCom
    from airflow.models.dag import DAG, DagModel
    from airflow.models.dagrun import DagRun
    from airflow.models.dagwarning import DagWarning
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.models.taskinstance import (
        TaskInstance,
        _add_log,
        _defer_task,
        _get_template_context,
        _handle_failure,
        _handle_reschedule,
        _update_rtif,
        _xcom_pull,
    )
    from airflow.providers.remote.models.remote_job import RemoteJob
    from airflow.secrets.metastore import MetastoreBackend
    from airflow.utils.cli_action_loggers import _default_action_log_internal
    from airflow.utils.log.file_task_handler import FileTaskHandler

    functions: list[Callable] = [
        _default_action_log_internal,
        _defer_task,
        _get_template_context,
        _get_ti_db_access,
        _update_rtif,
        _orig_start_date,
        _handle_failure,
        _handle_reschedule,
        _add_log,
        _xcom_pull,
        DagFileProcessor.update_import_errors,
        DagFileProcessor.manage_slas,
        DagFileProcessorManager.deactivate_stale_dags,
        DagModel.deactivate_deleted_dags,
        DagModel.get_paused_dag_ids,
        DagModel.get_current,
        DagFileProcessorManager.clear_nonexistent_import_errors,
        DagWarning.purge_inactive_dag_warnings,
        DatasetManager.register_dataset_change,
        FileTaskHandler._render_filename_db_access,
        Job._add_to_db,
        Job._fetch_from_db,
        Job._kill,
        Job._update_heartbeat,
        Job._update_in_db,
        most_recent_job,
        MetastoreBackend._fetch_connection,
        MetastoreBackend._fetch_variable,
        XCom.get_value,
        XCom.get_one,
        XCom.get_many,
        XCom.clear,
        XCom.set,
        Variable.set,
        Variable.update,
        Variable.delete,
        DAG.fetch_callback,
        DAG.fetch_dagrun,
        DagRun.fetch_task_instances,
        DagRun.get_previous_dagrun,
        DagRun.get_previous_scheduled_dagrun,
        DagRun.fetch_task_instance,
        DagRun._get_log_template,
        SerializedDagModel.get_serialized_dag,
        TaskInstance._check_and_change_state_before_execution,
        TaskInstance.get_task_instance,
        TaskInstance._get_dagrun,
        TaskInstance._set_state,
        TaskInstance.save_to_db,
        TaskInstance._schedule_downstream_tasks,
        TaskInstance._clear_xcom_data,
        Trigger.from_object,
        Trigger.bulk_fetch,
        Trigger.clean_unused,
        Trigger.submit_event,
        Trigger.submit_failure,
        Trigger.ids_for_triggerer,
        Trigger.assign_unassigned,
        # Additional things from Remote Executor
        RemoteJob.reserve_task,
        RemoteJob.set_state,
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

    methods_map = _initialize_map()
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
