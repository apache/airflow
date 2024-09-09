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

from flask import Response, request
from itsdangerous import BadSignature
from jwt import (
    ExpiredSignatureError,
    ImmatureSignatureError,
    InvalidAudienceError,
    InvalidIssuedAtError,
    InvalidSignatureError,
)

from airflow.api_connexion.exceptions import PermissionDenied
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.jobs.job import Job, most_recent_job
from airflow.models.dagcode import DagCode
from airflow.models.taskinstance import _record_task_map_for_downstreams
from airflow.models.xcom_arg import _get_task_map_length
from airflow.sensors.base import _orig_start_date
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.jwt_signer import JWTSigner
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.api_connexion.types import APIResponse

log = logging.getLogger(__name__)


@functools.lru_cache
def initialize_method_map() -> dict[str, Callable]:
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.cli.commands.task_command import _get_ti_db_access
    from airflow.dag_processing.manager import DagFileProcessorManager
    from airflow.dag_processing.processor import DagFileProcessor
    from airflow.datasets import expand_alias_to_datasets
    from airflow.datasets.manager import DatasetManager
    from airflow.models import Trigger, Variable, XCom
    from airflow.models.dag import DAG, DagModel
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
        _update_rtif,
        _xcom_pull,
    )
    from airflow.secrets.metastore import MetastoreBackend
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
        DagFileProcessor.manage_slas,
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
        MetastoreBackend._fetch_connection,
        MetastoreBackend._fetch_variable,
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
    ]
    return {f"{func.__module__}.{func.__qualname__}": func for func in functions}


def log_and_build_error_response(message, status):
    error_id = uuid4()
    server_message = message + f" error_id={error_id}"
    log.exception(server_message)
    client_message = message + f" The server side traceback may be identified with error_id={error_id}"
    return Response(response=client_message, status=status)


def internal_airflow_api(body: dict[str, Any]) -> APIResponse:
    """Handle Internal API /internal_api/v1/rpcapi endpoint."""
    content_type = request.headers.get("Content-Type")
    if content_type != "application/json":
        raise PermissionDenied("Expected Content-Type: application/json")
    accept = request.headers.get("Accept")
    if accept != "application/json":
        raise PermissionDenied("Expected Accept: application/json")
    auth = request.headers.get("Authorization", "")
    clock_grace = conf.getint("core", "internal_api_clock_grace", fallback=30)
    signer = JWTSigner(
        secret_key=conf.get("core", "internal_api_secret_key"),
        expiration_time_in_seconds=clock_grace,
        leeway_in_seconds=clock_grace,
        audience="api",
    )
    try:
        payload = signer.verify_token(auth)
        signed_method = payload.get("method")
        if not signed_method or signed_method != body.get("method"):
            raise BadSignature("Invalid method in token authorization.")
    except BadSignature:
        raise PermissionDenied("Bad Signature. Please use only the tokens provided by the API.")
    except InvalidAudienceError:
        raise PermissionDenied("Invalid audience for the request")
    except InvalidSignatureError:
        raise PermissionDenied("The signature of the request was wrong")
    except ImmatureSignatureError:
        raise PermissionDenied("The signature of the request was sent from the future")
    except ExpiredSignatureError:
        raise PermissionDenied(
            "The signature of the request has expired. Make sure that all components "
            "in your system have synchronized clocks.",
        )
    except InvalidIssuedAtError:
        raise PermissionDenied(
            "The request was issues in the future. Make sure that all components "
            "in your system have synchronized clocks.",
        )
    except Exception:
        raise PermissionDenied("Unable to authenticate API via token.")

    log.debug("Got request")
    json_rpc = body.get("jsonrpc")
    if json_rpc != "2.0":
        return log_and_build_error_response(message="Expected jsonrpc 2.0 request.", status=400)

    methods_map = initialize_method_map()
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

    log.info("Calling method %s\nparams: %s", method_name, params)
    try:
        # Session must be created there as it may be needed by serializer for lazy-loaded fields.
        with create_session() as session:
            output = handler(**params, session=session)
            output_json = BaseSerialization.serialize(output, use_pydantic_models=True)
            response = json.dumps(output_json) if output_json is not None else None
            log.info("Sending response: %s", response)
            return Response(response=response, headers={"Content-Type": "application/json"})
    # In case of AirflowException or other selective known types, transport the exception class back to caller
    except (KeyError, AttributeError, AirflowException) as e:
        exception_json = BaseSerialization.serialize(e, use_pydantic_models=True)
        response = json.dumps(exception_json)
        log.info("Sending exception response: %s", response)
        return Response(response=response, headers={"Content-Type": "application/json"})
    except Exception:
        return log_and_build_error_response(message=f"Error executing method '{method_name}'.", status=500)
