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

from flask import Response

from airflow.jobs.job import Job, most_recent_job
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.api_connexion.types import APIResponse

log = logging.getLogger(__name__)


@functools.lru_cache
def _initialize_map() -> dict[str, Callable]:
    from airflow.dag_processing.manager import DagFileProcessorManager
    from airflow.dag_processing.processor import DagFileProcessor
    from airflow.models import Trigger, Variable, XCom
    from airflow.models.dag import DAG, DagModel
    from airflow.models.dagrun import DagRun
    from airflow.models.dagwarning import DagWarning
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.models.taskinstance import TaskInstance
    from airflow.secrets.metastore import MetastoreBackend

    functions: list[Callable] = [
        DagFileProcessor.update_import_errors,
        DagFileProcessor.manage_slas,
        DagFileProcessorManager.deactivate_stale_dags,
        DagModel.deactivate_deleted_dags,
        DagModel.get_paused_dag_ids,
        DagModel.get_current,
        DagFileProcessorManager.clear_nonexistent_import_errors,
        DagWarning.purge_inactive_dag_warnings,
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
        TaskInstance.fetch_handle_failure_context,
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
    ]
    return {f"{func.__module__}.{func.__qualname__}": func for func in functions}


def internal_airflow_api(body: dict[str, Any]) -> APIResponse:
    """Handle Internal API /internal_api/v1/rpcapi endpoint."""
    log.debug("Got request")
    json_rpc = body.get("jsonrpc")
    if json_rpc != "2.0":
        log.error("Not jsonrpc-2.0 request.")
        return Response(response="Expected jsonrpc 2.0 request.", status=400)

    methods_map = _initialize_map()
    method_name = body.get("method")
    if method_name not in methods_map:
        log.error("Unrecognized method: %s.", method_name)
        return Response(response=f"Unrecognized method: {method_name}.", status=400)

    handler = methods_map[method_name]
    params = {}
    try:
        if body.get("params"):
            params_json = json.loads(str(body.get("params")))
            params = BaseSerialization.deserialize(params_json, use_pydantic_models=True)
    except Exception as e:
        log.error("Error when deserializing parameters for method: %s.", method_name)
        log.exception(e)
        return Response(response="Error deserializing parameters.", status=400)

    log.debug("Calling method %s.", method_name)
    try:
        # Session must be created there as it may be needed by serializer for lazy-loaded fields.
        with create_session() as session:
            output = handler(**params, session=session)
            output_json = BaseSerialization.serialize(output, use_pydantic_models=True)
            response = json.dumps(output_json) if output_json is not None else None
            return Response(response=response, headers={"Content-Type": "application/json"})
    except Exception as e:
        log.error("Error executing method: %s.", method_name)
        log.exception(e)
        return Response(response=f"Error executing method: {method_name}.", status=500)
