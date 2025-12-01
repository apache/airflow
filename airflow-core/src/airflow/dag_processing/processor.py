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

import contextlib
import importlib
import os
import sys
import traceback
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, BinaryIO, ClassVar, Literal

import attrs
from pydantic import BaseModel, Field, TypeAdapter

from airflow.callbacks.callback_requests import (
    CallbackRequest,
    DagCallbackRequest,
    EmailRequest,
    TaskCallbackRequest,
)
from airflow.configuration import conf
from airflow.dag_processing.dagbag import DagBag
from airflow.sdk.exceptions import TaskNotFound
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    DeleteVariable,
    ErrorResponse,
    GetConnection,
    GetPreviousDagRun,
    GetPrevSuccessfulDagRun,
    GetTaskStates,
    GetTICount,
    GetVariable,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    MaskSecret,
    OKResponse,
    PreviousDagRunResult,
    PrevSuccessfulDagRunResult,
    PutVariable,
    TaskStatesResult,
    VariableResult,
    XComCountResponse,
    XComResult,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.supervisor import WatchedSubprocess
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, _send_error_email_notification
from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedDAG
from airflow.stats import Stats
from airflow.utils.file import iter_airflow_imports
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
    from airflow.sdk.api.client import Client
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.typing_compat import Self


class DagFileParseRequest(BaseModel):
    """
    Request for DAG File Parsing.

    This is the request that the manager will send to the DAG parser with the dag file and
    any other necessary metadata.
    """

    file: str

    bundle_path: Path
    """Passing bundle path around lets us figure out relative file path."""

    bundle_name: str
    """Bundle name for team-specific executor validation."""

    callback_requests: list[CallbackRequest] = Field(default_factory=list)
    type: Literal["DagFileParseRequest"] = "DagFileParseRequest"


class DagFileParsingResult(BaseModel):
    """
    Result of DAG File Parsing.

    This is the result of a successful DAG parse, in this class, we gather all serialized DAGs,
    import errors and warnings to send back to the scheduler to store in the DB.
    """

    fileloc: str
    serialized_dags: list[LazyDeserializedDAG]
    warnings: list | None = None
    import_errors: dict[str, str] | None = None
    type: Literal["DagFileParsingResult"] = "DagFileParsingResult"


ToManager = Annotated[
    DagFileParsingResult
    | GetConnection
    | GetVariable
    | PutVariable
    | GetTaskStates
    | GetTICount
    | DeleteVariable
    | GetPrevSuccessfulDagRun
    | GetPreviousDagRun
    | GetXCom
    | GetXComCount
    | GetXComSequenceItem
    | GetXComSequenceSlice
    | MaskSecret,
    Field(discriminator="type"),
]

ToDagProcessor = Annotated[
    DagFileParseRequest
    | ConnectionResult
    | VariableResult
    | TaskStatesResult
    | PreviousDagRunResult
    | PrevSuccessfulDagRunResult
    | ErrorResponse
    | OKResponse
    | XComCountResponse
    | XComResult
    | XComSequenceIndexResult
    | XComSequenceSliceResult,
    Field(discriminator="type"),
]


def _pre_import_airflow_modules(file_path: str, log: FilteringBoundLogger) -> None:
    """
    Pre-import Airflow modules found in the given file.

    This prevents modules from being re-imported in each processing process,
    saving CPU time and memory.
    (The default value of "parsing_pre_import_modules" is set to True)

    :param file_path: Path to the file to scan for imports
    :param log: Logger instance to use for warnings
    """
    if not conf.getboolean("dag_processor", "parsing_pre_import_modules", fallback=True):
        return

    for module in iter_airflow_imports(file_path):
        try:
            importlib.import_module(module)
        except Exception as e:
            log.warning("Error when trying to pre-import module '%s' found in %s: %s", module, file_path, e)


def _parse_file_entrypoint():
    # Mark as client-side (runs user DAG code)
    # Prevents inheriting server context from parent DagProcessorManager
    os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "client"

    import structlog

    from airflow.sdk.execution_time import comms, task_runner

    # Parse DAG file, send JSON back up!
    comms_decoder = comms.CommsDecoder[ToDagProcessor, ToManager](
        body_decoder=TypeAdapter[ToDagProcessor](ToDagProcessor),
    )

    msg = comms_decoder._get_response()
    if not isinstance(msg, DagFileParseRequest):
        raise RuntimeError(f"Required first message to be a DagFileParseRequest, it was {msg}")

    task_runner.SUPERVISOR_COMMS = comms_decoder
    log = structlog.get_logger(logger_name="task")

    # Put bundle root on sys.path if needed. This allows the dag bundle to add
    # code in util modules to be shared between files within the same bundle.
    if (bundle_root := os.fspath(msg.bundle_path)) not in sys.path:
        sys.path.append(bundle_root)

    result = _parse_file(msg, log)
    if result is not None:
        comms_decoder.send(result)


def _parse_file(msg: DagFileParseRequest, log: FilteringBoundLogger) -> DagFileParsingResult | None:
    # TODO: Set known_pool names on DagBag!

    bag = DagBag(
        dag_folder=msg.file,
        bundle_path=msg.bundle_path,
        bundle_name=msg.bundle_name,
        include_examples=False,
        load_op_links=False,
    )
    if msg.callback_requests:
        # If the request is for callback, we shouldn't serialize the DAGs
        _execute_callbacks(bag, msg.callback_requests, log)
        return None

    serialized_dags, serialization_import_errors = _serialize_dags(bag, log)
    bag.import_errors.update(serialization_import_errors)
    result = DagFileParsingResult(
        fileloc=msg.file,
        serialized_dags=serialized_dags,
        import_errors=bag.import_errors,
        # TODO: Make `bag.dag_warnings` not return SQLA model objects
        warnings=[],
    )
    return result


def _serialize_dags(
    bag: DagBag,
    log: FilteringBoundLogger,
) -> tuple[list[LazyDeserializedDAG], dict[str, str]]:
    serialization_import_errors = {}
    serialized_dags = []
    for dag in bag.dags.values():
        try:
            data = SerializedDAG.to_dict(dag)
            serialized_dags.append(LazyDeserializedDAG(data=data, last_loaded=dag.last_loaded))
        except Exception:
            log.exception("Failed to serialize DAG: %s", dag.fileloc)
            dagbag_import_error_traceback_depth = conf.getint(
                "core", "dagbag_import_error_traceback_depth", fallback=None
            )
            serialization_import_errors[dag.fileloc] = traceback.format_exc(
                limit=-dagbag_import_error_traceback_depth
            )
    return serialized_dags, serialization_import_errors


def _get_dag_with_task(
    dagbag: DagBag, dag_id: str, task_id: str | None = None
) -> tuple[DAG, BaseOperator | MappedOperator | None]:
    """
    Retrieve a DAG and optionally a task from the DagBag.

    :param dagbag: DagBag to retrieve from
    :param dag_id: DAG ID to retrieve
    :param task_id: Optional task ID to retrieve from the DAG
    :return: tuple of (dag, task) where task is None if not requested
    :raises ValueError: If DAG or task is not found
    """
    if dag_id not in dagbag.dags:
        raise ValueError(
            f"DAG '{dag_id}' not found in DagBag. "
            f"This typically indicates a race condition where the DAG was removed or failed to parse."
        )

    dag = dagbag.dags[dag_id]

    if task_id is not None:
        try:
            task = dag.get_task(task_id)
            return dag, task
        except TaskNotFound:
            raise ValueError(
                f"Task '{task_id}' not found in DAG '{dag_id}'. "
                f"This typically indicates a race condition where the task was removed or the DAG structure changed."
            ) from None

    return dag, None


def _execute_callbacks(
    dagbag: DagBag, callback_requests: list[CallbackRequest], log: FilteringBoundLogger
) -> None:
    for request in callback_requests:
        log.debug("Processing Callback Request", request=request.to_json())
        if isinstance(request, TaskCallbackRequest):
            _execute_task_callbacks(dagbag, request, log)
        elif isinstance(request, DagCallbackRequest):
            _execute_dag_callbacks(dagbag, request, log)
        elif isinstance(request, EmailRequest):
            _execute_email_callbacks(dagbag, request, log)


def _execute_dag_callbacks(dagbag: DagBag, request: DagCallbackRequest, log: FilteringBoundLogger) -> None:
    from airflow.sdk.api.datamodels._generated import TIRunContext

    dag, _ = _get_dag_with_task(dagbag, request.dag_id)
    callbacks = dag.on_failure_callback if request.is_failure_callback else dag.on_success_callback
    if not callbacks:
        log.warning("Callback requested, but dag didn't have any", dag_id=request.dag_id)
        return

    callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
    ctx_from_server = request.context_from_server

    if ctx_from_server is not None and ctx_from_server.last_ti is not None:
        task = dag.get_task(ctx_from_server.last_ti.task_id)

        runtime_ti = RuntimeTaskInstance.model_construct(
            **ctx_from_server.last_ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=TIRunContext.model_construct(
                dag_run=ctx_from_server.dag_run,
                max_tries=task.retries,
            ),
        )
        context = runtime_ti.get_template_context()
        context["reason"] = request.msg
    else:
        context: Context = {  # type: ignore[no-redef]
            "dag": dag,
            "run_id": request.run_id,
            "reason": request.msg,
        }

    for callback in callbacks:
        log.info(
            "Executing on_%s dag callback",
            "failure" if request.is_failure_callback else "success",
            dag_id=request.dag_id,
        )
        try:
            callback(context)
        except Exception:
            log.exception("Callback failed", dag_id=request.dag_id)
            Stats.incr("dag.callback_exceptions", tags={"dag_id": request.dag_id})


def _execute_task_callbacks(dagbag: DagBag, request: TaskCallbackRequest, log: FilteringBoundLogger) -> None:
    if not request.is_failure_callback:
        log.warning(
            "Task callback requested but is not a failure callback",
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
        )
        return

    dag, task = _get_dag_with_task(dagbag, request.ti.dag_id, request.ti.task_id)

    if TYPE_CHECKING:
        assert task is not None

    if request.task_callback_type is TaskInstanceState.UP_FOR_RETRY:
        callbacks = task.on_retry_callback
    else:
        callbacks = task.on_failure_callback

    if not callbacks:
        log.warning(
            "Callback requested but no callback found",
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
            ti_id=request.ti.id,
        )
        return

    callbacks = callbacks if isinstance(callbacks, Sequence) else [callbacks]
    ctx_from_server = request.context_from_server

    if ctx_from_server is not None:
        runtime_ti = RuntimeTaskInstance.model_construct(
            **request.ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=ctx_from_server,
            max_tries=ctx_from_server.max_tries,
        )
    else:
        runtime_ti = RuntimeTaskInstance.model_construct(
            **request.ti.model_dump(exclude_unset=True),
            task=task,
        )
    context = runtime_ti.get_template_context()

    def get_callback_representation(callback):
        with contextlib.suppress(AttributeError):
            return callback.__name__
        with contextlib.suppress(AttributeError):
            return callback.__class__.__name__
        return callback

    for idx, callback in enumerate(callbacks):
        callback_repr = get_callback_representation(callback)
        log.info("Executing Task callback at index %d: %s", idx, callback_repr)
        try:
            callback(context)
        except Exception:
            log.exception("Error in callback at index %d: %s", idx, callback_repr)


def _execute_email_callbacks(dagbag: DagBag, request: EmailRequest, log: FilteringBoundLogger) -> None:
    """Execute email notification for task failure/retry."""
    dag, task = _get_dag_with_task(dagbag, request.ti.dag_id, request.ti.task_id)

    if TYPE_CHECKING:
        assert task is not None

    if not task.email:
        log.warning(
            "Email callback requested but no email configured",
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
        )
        return

    # Check if email should be sent based on task configuration
    should_send_email = False
    if request.email_type == "failure" and task.email_on_failure:
        should_send_email = True
    elif request.email_type == "retry" and task.email_on_retry:
        should_send_email = True

    if not should_send_email:
        log.info(
            "Email not sent - task configured with email_on_%s=False",
            request.email_type,
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
        )
        return

    ctx_from_server = request.context_from_server

    runtime_ti = RuntimeTaskInstance.model_construct(
        **request.ti.model_dump(exclude_unset=True),
        task=task,
        _ti_context_from_server=ctx_from_server,
        max_tries=ctx_from_server.max_tries,
    )

    log.info(
        "Sending %s email for task %s",
        request.email_type,
        request.ti.task_id,
        dag_id=request.ti.dag_id,
        run_id=request.ti.run_id,
    )

    try:
        context = runtime_ti.get_template_context()
        error = Exception(request.msg) if request.msg else None
        _send_error_email_notification(task, runtime_ti, context, error, log)
    except Exception:
        log.exception(
            "Failed to send %s email",
            request.email_type,
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
        )


def in_process_api_server() -> InProcessExecutionAPI:
    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI

    api = InProcessExecutionAPI()
    return api


@attrs.define(kw_only=True)
class DagFileProcessorProcess(WatchedSubprocess):
    """
    Parses dags with Task SDK API.

    This class provides a wrapper and management around a subprocess to parse a specific DAG file.

    Since DAGs are written with the Task SDK, we need to parse them in a task SDK process such that
    we can use the Task SDK definitions when serializing. This prevents potential conflicts with classes
    in core Airflow.
    """

    logger_filehandle: BinaryIO
    parsing_result: DagFileParsingResult | None = None
    decoder: ClassVar[TypeAdapter[ToManager]] = TypeAdapter[ToManager](ToManager)
    had_callbacks: bool = False  # Track if this process was started with callbacks to prevent stale DAG detection false positives

    client: Client
    """The HTTP client to use for communication with the API server."""

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
        callbacks: list[CallbackRequest],
        target: Callable[[], None] = _parse_file_entrypoint,
        client: Client,
        **kwargs,
    ) -> Self:
        logger = kwargs["logger"]

        _pre_import_airflow_modules(os.fspath(path), logger)

        proc: Self = super().start(target=target, client=client, **kwargs)
        proc.had_callbacks = bool(callbacks)  # Track if this process had callbacks
        proc._on_child_started(callbacks, path, bundle_path, bundle_name)
        return proc

    def _on_child_started(
        self,
        callbacks: list[CallbackRequest],
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
    ) -> None:
        msg = DagFileParseRequest(
            file=os.fspath(path),
            bundle_path=bundle_path,
            bundle_name=bundle_name,
            callback_requests=callbacks,
        )
        self.send_msg(msg, request_id=0)

    def _handle_request(self, msg: ToManager, log: FilteringBoundLogger, req_id: int) -> None:
        from airflow.sdk.api.datamodels._generated import (
            ConnectionResponse,
            TaskStatesResponse,
            VariableResponse,
            XComSequenceIndexResponse,
        )

        resp: BaseModel | None = None
        dump_opts = {}
        if isinstance(msg, DagFileParsingResult):
            self.parsing_result = msg
        elif isinstance(msg, GetConnection):
            conn = self.client.connections.get(msg.conn_id)
            if isinstance(conn, ConnectionResponse):
                conn_result = ConnectionResult.from_conn_response(conn)
                resp = conn_result
                dump_opts = {"exclude_unset": True, "by_alias": True}
            else:
                resp = conn
        elif isinstance(msg, GetVariable):
            var = self.client.variables.get(msg.key)
            if isinstance(var, VariableResponse):
                var_result = VariableResult.from_variable_response(var)
                resp = var_result
                dump_opts = {"exclude_unset": True}
            else:
                resp = var
        elif isinstance(msg, PutVariable):
            self.client.variables.set(msg.key, msg.value, msg.description)
        elif isinstance(msg, DeleteVariable):
            resp = self.client.variables.delete(msg.key)
        elif isinstance(msg, GetPreviousDagRun):
            resp = self.client.dag_runs.get_previous(
                dag_id=msg.dag_id,
                logical_date=msg.logical_date,
                state=msg.state,
            )
        elif isinstance(msg, GetPrevSuccessfulDagRun):
            dagrun_resp = self.client.task_instances.get_previous_successful_dagrun(self.id)
            dagrun_result = PrevSuccessfulDagRunResult.from_dagrun_response(dagrun_resp)
            resp = dagrun_result
            dump_opts = {"exclude_unset": True}
        elif isinstance(msg, GetXCom):
            xcom = self.client.xcoms.get(
                msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.map_index, msg.include_prior_dates
            )
            xcom_result = XComResult.from_xcom_response(xcom)
            resp = xcom_result
        elif isinstance(msg, GetXComCount):
            resp = self.client.xcoms.head(msg.dag_id, msg.run_id, msg.task_id, msg.key)
        elif isinstance(msg, GetXComSequenceItem):
            xcom = self.client.xcoms.get_sequence_item(
                msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.offset
            )
            if isinstance(xcom, XComSequenceIndexResponse):
                resp = XComSequenceIndexResult.from_response(xcom)
            else:
                resp = xcom
        elif isinstance(msg, GetXComSequenceSlice):
            xcoms = self.client.xcoms.get_sequence_slice(
                msg.dag_id,
                msg.run_id,
                msg.task_id,
                msg.key,
                msg.start,
                msg.stop,
                msg.step,
                msg.include_prior_dates,
            )
            resp = XComSequenceSliceResult.from_response(xcoms)
        elif isinstance(msg, MaskSecret):
            # Use sdk masker in dag processor and triggerer because those use the task sdk machinery
            from airflow.sdk.log import mask_secret

            mask_secret(msg.value, msg.name)
        elif isinstance(msg, GetTICount):
            resp = self.client.task_instances.get_count(
                dag_id=msg.dag_id,
                map_index=msg.map_index,
                task_ids=msg.task_ids,
                task_group_id=msg.task_group_id,
                logical_dates=msg.logical_dates,
                run_ids=msg.run_ids,
                states=msg.states,
            )
        elif isinstance(msg, GetTaskStates):
            task_states_map = self.client.task_instances.get_task_states(
                dag_id=msg.dag_id,
                map_index=msg.map_index,
                task_ids=msg.task_ids,
                task_group_id=msg.task_group_id,
                logical_dates=msg.logical_dates,
                run_ids=msg.run_ids,
            )
            if isinstance(task_states_map, TaskStatesResponse):
                resp = TaskStatesResult.from_api_response(task_states_map)
            else:
                resp = task_states_map
        else:
            log.error("Unhandled request", msg=msg)
            self.send_msg(
                None,
                request_id=req_id,
                error=ErrorResponse(
                    detail={"status_code": 400, "message": "Unhandled request"},
                ),
            )
            return

        self.send_msg(resp, request_id=req_id, error=None, **dump_opts)

    @property
    def is_ready(self) -> bool:
        if self._check_subprocess_exit() is None:
            # Process still alive, def can't be finished yet
            return False

        return not self._open_sockets

    def wait(self) -> int:
        raise NotImplementedError(f"Don't call wait on {type(self).__name__} objects")
