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
import logging
import os
import time
import traceback
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, BinaryIO, ClassVar, Literal

import attrs
import psutil
from pydantic import BaseModel, Field, TypeAdapter

from airflow._shared.observability.metrics import stats
from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import ArgValueSchema  # noqa: TC001
from airflow.callbacks.callback_requests import (
    CallbackRequest,
    DagCallbackRequest,
    EmailRequest,
    TaskCallbackRequest,
)
from airflow.configuration import conf
from airflow.dag_processing.bundles.base import BundleVersionLock
from airflow.dag_processing.dagbag import BundleDagBag, DagBag
from airflow.exceptions import AirflowConfigException
from airflow.models.dag import DagModel
from airflow.sdk.exceptions import TaskNotFound
from airflow.sdk.execution_time import supervisor
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    DeleteVariable,
    ErrorResponse,
    GetConnection,
    GetPreviousDagRun,
    GetPreviousTI,
    GetPrevSuccessfulDagRun,
    GetTaskStates,
    GetTICount,
    GetVariable,
    GetVariableKeys,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    MaskSecret,
    OKResponse,
    PreviousDagRunResult,
    PreviousTIResult,
    PrevSuccessfulDagRunResult,
    PutVariable,
    TaskStatesResult,
    VariableKeysResult,
    VariableResult,
    XComCountResponse,
    XComResult,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.supervisor import WatchedSubprocess, register_request_method
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, _send_error_email_notification
from airflow.sdk.importers import DagSourceCode  # noqa: TC001
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
from airflow.utils.dag_version_inflation_checker import check_dag_file_stability
from airflow.utils.file import iter_airflow_imports
from airflow.utils.helpers import prune_dict
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from socket import socket

    from structlog.typing import FilteringBoundLogger

    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
    from airflow.dag_processing.task_handler_resolution import TaskHandlerResolution
    from airflow.sdk.api.client import Client
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.sdk.execution_time.supervisor import RequestHandler, RequestResult, ResponseSent
    from airflow.typing_compat import Self


TaskHandlerBindingMode = Literal["positional", "named"]


class TaskHandlerParam(BaseModel):
    """One parameter of a task handler."""

    name: str | None
    """``None`` when the runtime has no name for this positional parameter."""

    value_schema: ArgValueSchema | None = None
    """JSON Schema of the values the parameter accepts; ``None`` when the handler does not constrain it."""

    exact_name: bool = False
    """Whether ``name`` matches only as spelled, not case-insensitively with underscores ignored."""


class TaskHandlerDeclaration(BaseModel):
    """A task handler that a Lang-SDK artifact registers for one task."""

    task_id: str

    binding: TaskHandlerBindingMode
    """
    How stub-task arguments bind to ``params``.

    - ``positional``: by position; names are informative only. An argument count that matches ``params``
      neither with every argument nor after dropping the defaulted ones, or a value type the param does not
      accept, makes the Dag fail to import.
    - ``named``: by name in any order, case-insensitively with underscores ignored unless ``exact_name`` is set.
      An argument no param takes, or a param no argument fills, is logged as a warning, and the task still
      runs. When no param matches and exactly one argument was passed, it may be the whole value and is not
      warned about, unless ``params`` is empty, a param sets ``exact_name``, or the argument cannot be an
      object. A value type a param does not accept makes the Dag fail to import.
    """

    # The title keeps Go's generated type for this list apart from the HITL ``Params`` map.
    params: Annotated[list[TaskHandlerParam] | None, Field(title="Task Handler Params")]
    """
    In declaration order; the order is significant only for ``positional`` binding.

    ``None`` when the runtime cannot list the handler's parameters, so only the handler's presence is checked.
    """


class TaskHandlerArtifact(BaseModel):
    """A Lang-SDK artifact and every task handler it registers."""

    bundle_name: str

    relative_fileloc: str = Field(max_length=2000)
    """Path of the artifact within its bundle."""

    size_bytes: int

    cache_digest: str | None = Field(max_length=128)
    """Opaque content fingerprint defined by the coordinator; ``None`` when the artifact stores none, so it is always probed."""

    task_handlers: dict[str, list[TaskHandlerDeclaration]]
    """Every Dag id the artifact registers a task handler for, with those handlers."""


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

    known_artifacts: list[TaskHandlerArtifact] = Field(default_factory=list)
    """The recorded task-handler artifacts, with their answers, that this file's stub tasks may resolve against."""

    type: Literal["DagFileParseRequest"] = "DagFileParseRequest"


class TaskHandlerBinding(BaseModel):
    """A stub task resolved to the Lang-SDK artifact that runs it."""

    dag_id: str
    task_id: str

    artifact_bundle_name: str
    """The bundle the artifact was found in."""

    artifact_rel_path: str = Field(max_length=2000)
    """Path of the artifact within its bundle."""


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
    parsed_definitions: list[str] = Field(default_factory=list)
    """Bundle-relative locations of the Dag definitions imported from ``fileloc``."""
    dag_source_codes: dict[str, DagSourceCode] = Field(default_factory=dict)
    """Source code of the parsed Dags, keyed by Dag fileloc."""

    task_handler_bindings: list[TaskHandlerBinding] | None = None
    """
    The stub-task bindings of every Dag in ``serialized_dags``.

    ``None`` when task handlers were not evaluated, and the recorded bindings are left as they are. A list
    replaces the recorded bindings of each Dag in ``serialized_dags``, so a Dag with no entry has none.
    """

    probed_artifacts: list[TaskHandlerArtifact] = Field(default_factory=list)
    """
    Every artifact this parse probed successfully, with its answer.

    Recorded even when ``task_handler_bindings`` is ``None``, so a Dag that fails validation is not probed
    again on every parse.
    """

    type: Literal["DagFileParsingResult"] = "DagFileParsingResult"


class TaskHandlerParseRequest(BaseModel):
    """
    Request for Task Handler Parsing.

    Asks a Lang-SDK runtime for every task handler an artifact registers.
    """

    file: str
    """The artifact to ask."""

    bundle_path: Path

    bundle_name: str

    type: Literal["TaskHandlerParseRequest"] = "TaskHandlerParseRequest"


class TaskHandlerParsingResult(BaseModel):
    """
    Result of Task Handler Parsing.

    Every task handler a Lang-SDK artifact registers, keyed by Dag id.

    The answer depends only on the artifact, never on the request.
    """

    fileloc: str

    task_handlers: dict[str, list[TaskHandlerDeclaration]]
    """Every Dag id the artifact registers a task handler for; ``{}`` when it registers none."""

    import_errors: dict[str, str] | None = None
    warnings: list | None = None
    type: Literal["TaskHandlerParsingResult"] = "TaskHandlerParsingResult"


ToManager = Annotated[
    DagFileParsingResult
    | TaskHandlerParsingResult
    | GetConnection
    | GetVariable
    | GetVariableKeys
    | PutVariable
    | GetTaskStates
    | GetTICount
    | DeleteVariable
    | GetPrevSuccessfulDagRun
    | GetPreviousDagRun
    | GetPreviousTI
    | GetXCom
    | GetXComCount
    | GetXComSequenceItem
    | GetXComSequenceSlice
    | MaskSecret,
    Field(discriminator="type"),
]

# Answers to the child's requests, whichever parse it was started for.
_ParseSideResponses = (
    ConnectionResult
    | VariableResult
    | VariableKeysResult
    | TaskStatesResult
    | PreviousDagRunResult
    | PreviousTIResult
    | PrevSuccessfulDagRunResult
    | ErrorResponse
    | OKResponse
    | XComCountResponse
    | XComResult
    | XComSequenceIndexResult
    | XComSequenceSliceResult
)

ToDagProcessor = Annotated[DagFileParseRequest | _ParseSideResponses, Field(discriminator="type")]

ToSDKTaskHandlerProcessor = Annotated[
    TaskHandlerParseRequest | _ParseSideResponses, Field(discriminator="type")
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

    result = _parse_file(msg, log, started=_get_process_start())

    if result is not None:
        comms_decoder.send(result)


def _parse_file(
    msg: DagFileParseRequest, log: FilteringBoundLogger, *, started: float | None = None
) -> DagFileParsingResult | None:
    """
    Parse the Dag file of *msg* and return the result to send, or ``None`` for a callback request.

    *started* is when the parse began, a :func:`time.monotonic` value that the check of the stub tasks counts
    its time budget from. It defaults to the call of this function.
    """
    # TODO: Set known_pool names on DagBag!
    if started is None:
        started = time.monotonic()

    stability_check_result = check_dag_file_stability(os.fspath(msg.file))

    # Callback runs must not be blocked by the stability check: callbacks for
    # already-scheduled runs still have to execute, and they never produce a
    # parsing result anyway.
    if not msg.callback_requests and (
        stability_check_error_dict := stability_check_result.get_error_format_dict(msg.file, msg.bundle_path)
    ):
        # If Dag stability check level is error, we shouldn't parse the Dags and return the result early
        return DagFileParsingResult(
            fileloc=msg.file,
            serialized_dags=[],
            import_errors=stability_check_error_dict,
        )

    bag = BundleDagBag(
        dag_folder=msg.file,
        bundle_path=msg.bundle_path,
        bundle_name=msg.bundle_name,
        load_op_links=False,
    )

    if msg.callback_requests:
        # If the request is for callback, we shouldn't serialize the Dags
        _execute_callbacks(bag, msg.callback_requests, log)
        return None

    serialized_dags, serialization_import_errors = _serialize_dags(bag, log)
    bag.import_errors.update(serialization_import_errors)
    task_handlers = _resolve_task_handlers(msg, bag, serialized_dags, log, started=started)
    _add_import_errors(bag.import_errors, task_handlers.import_errors)
    result = DagFileParsingResult(
        fileloc=msg.file,
        serialized_dags=serialized_dags,
        import_errors=bag.import_errors,
        warnings=[
            *stability_check_result.get_formatted_warnings(bag.dag_ids),
            *(
                {"dag_id": w.dag_id, "warning_type": w.warning_type, "message": w.message}
                for w in bag.dag_warnings
            ),
        ],
        parsed_definitions=bag.parsed_definitions,
        dag_source_codes=bag.dag_source_codes,
        task_handler_bindings=task_handlers.bindings,
        probed_artifacts=task_handlers.probed_artifacts,
    )
    return result


def _get_process_start() -> float:
    """
    Return when this process was created, as a :func:`time.monotonic` value.

    The manager counts ``[dag_processor] dag_file_processor_timeout`` from the creation of the Dag-parsing
    child, so the start-up of an interpreter that is exec'd counts too. An age that is negative or larger than
    the timeout means the clocks disagree, and the process is counted from now, as it is when its creation
    time cannot be read. On Linux psutil computes the creation time from the boot time in whole seconds,
    so the age can be up to about 1 s too high, which only makes the deadline earlier.
    """
    now = time.monotonic()
    try:
        age = time.time() - psutil.Process().create_time()
    except (psutil.Error, OSError):
        return now
    if not 0 <= age <= conf.getfloat("dag_processor", "dag_file_processor_timeout"):
        return now
    return now - age


def _resolve_task_handlers(
    msg: DagFileParseRequest,
    bag: DagBag,
    serialized_dags: list[LazyDeserializedDAG],
    log: FilteringBoundLogger,
    *,
    started: float,
) -> TaskHandlerResolution:
    """
    Check the stub tasks of the serialized Dags whose queues ``[sdk] queue_to_coordinator`` routes.

    Nothing is checked without that option, and a stub task on another queue is left to a worker outside
    Airflow's coordinators. The check ends by 90% of ``[dag_processor] dag_file_processor_timeout`` from
    *started*, so the result is sent before the manager kills this process. An unexpected error is an
    import error of each Dag file with a checked stub task, so the serialized Dags are still sent.
    """
    # Imported here: the probe imports this module, and a Dag file without stub tasks needs neither.
    from airflow.dag_processing.task_handler_resolution import TaskHandlerResolution, resolve_task_handlers
    from airflow.dag_processing.task_handler_validation import collect_stub_tasks

    try:
        queue_to_coordinator = conf.getjson("sdk", "queue_to_coordinator", fallback={})
    except AirflowConfigException:
        queue_to_coordinator = None
    else:
        if not queue_to_coordinator:
            return TaskHandlerResolution(bindings=None, probed_artifacts=[], import_errors={})
    # An invalid option does not filter here: resolving the stub tasks reports it.
    routed_queues = queue_to_coordinator if isinstance(queue_to_coordinator, dict) else None
    stub_tasks = [
        stub
        for stub in collect_stub_tasks(bag.dags.values(), serialized_dags)
        if routed_queues is None or stub.queue in routed_queues
    ]
    if not stub_tasks:
        return TaskHandlerResolution(bindings=[], probed_artifacts=[], import_errors={})
    try:
        return resolve_task_handlers(
            stub_tasks,
            dag_bundle_name=msg.bundle_name,
            dag_bundle_path=msg.bundle_path,
            known_artifacts=msg.known_artifacts,
            deadline=started + 0.9 * conf.getfloat("dag_processor", "dag_file_processor_timeout"),
            log=log,
        )
    except Exception as e:
        log.exception("Failed to check the stub tasks against their task handlers", file=msg.file)
        return TaskHandlerResolution.failed(
            stub_tasks, f"Unexpected error while checking the stub tasks: {type(e).__name__}: {e}"
        )


def _add_import_errors(import_errors: dict[str, str], new_import_errors: dict[str, str]) -> None:
    """Add *new_import_errors* to *import_errors*, after a blank line in a file that already has one."""
    for fileloc, message in new_import_errors.items():
        if existing := import_errors.get(fileloc):
            import_errors[fileloc] = f"{existing.rstrip()}\n\n{message}"
        else:
            import_errors[fileloc] = message


def _serialize_dags(
    bag: DagBag,
    log: FilteringBoundLogger,
) -> tuple[list[LazyDeserializedDAG], dict[str, str]]:
    serialization_import_errors = {}
    serialized_dags = []
    for dag in bag.dags.values():
        try:
            data = DagSerialization.to_dict(dag)
            serialized_dags.append(LazyDeserializedDAG(data=data, last_loaded=dag.last_loaded))
        except Exception:
            log.exception("Failed to serialize DAG: %s", dag.fileloc)
            dagbag_import_error_traceback_depth = conf.getint(
                "core", "dagbag_import_error_traceback_depth", fallback=None
            )
            # Use relative_fileloc if available, fall back to fileloc
            error_path = dag.relative_fileloc or dag.fileloc
            serialization_import_errors[error_path] = traceback.format_exc(
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
        if isinstance(request, (TaskCallbackRequest, EmailRequest)):
            log_extra = {
                "dag_id": request.ti.dag_id,
                "run_id": request.ti.run_id,
                "ti_id": str(request.ti.id),
            }
        else:
            log_extra = {"dag_id": request.dag_id, "run_id": request.run_id}
        # context_from_server can carry user-supplied run conf, and the masker cannot
        # redact inside an already-serialized string, so keep it out of log payloads.
        request_json = request.to_json(exclude={"context_from_server"})
        log.debug("Processing Callback Request", request=request_json, **log_extra)
        # A failed request (e.g. the Dag or task was removed since the callback
        # was scheduled) must not abort the remaining requests in this batch --
        # they were already popped from the manager's queue and would be lost.
        try:
            with BundleVersionLock(
                bundle_name=request.bundle_name,
                bundle_version=request.bundle_version,
            ):
                if isinstance(request, TaskCallbackRequest):
                    _execute_task_callbacks(dagbag, request, log)
                elif isinstance(request, DagCallbackRequest):
                    _execute_dag_callbacks(dagbag, request, log)
                elif isinstance(request, EmailRequest):
                    _execute_email_callbacks(dagbag, request, log)
        except Exception:
            log.exception("Failed to execute callback request", request=request_json, **log_extra)


def _execute_dag_callbacks(dagbag: DagBag, request: DagCallbackRequest, log: FilteringBoundLogger) -> None:
    from airflow.sdk.api.datamodels._generated import TIRunContext

    dag, _ = _get_dag_with_task(dagbag, request.dag_id)
    callbacks = dag.on_failure_callback if request.is_failure_callback else dag.on_success_callback
    if not callbacks:
        log.warning("Callback requested, but dag didn't have any", dag_id=request.dag_id)
        return

    callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
    ctx_from_server = request.context_from_server

    context: Context = {
        "dag": dag,
        "run_id": request.run_id,
        "reason": request.msg,
    }
    if ctx_from_server is not None and ctx_from_server.last_ti is not None:
        try:
            task = dag.get_task(ctx_from_server.last_ti.task_id)
        except TaskNotFound:
            # The task only enriches the callback context; a task removed since the
            # run must not cost the user the callback itself (produce_dag_callback
            # makes the same call for an unrepresentable last_ti).
            log.warning(
                "Task from callback context no longer exists in the Dag; running callback with minimal context",
                dag_id=request.dag_id,
                task_id=ctx_from_server.last_ti.task_id,
            )
        else:
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
            stats.incr(
                "dag.callback_exceptions",
                tags=prune_dict(
                    {
                        "dag_id": request.dag_id,
                        "team_name": (
                            DagModel.get_team_name(request.dag_id)
                            if conf.getboolean("core", "multi_team")
                            else None
                        ),
                    }
                ),
            )


def _execute_task_callbacks(dagbag: DagBag, request: TaskCallbackRequest, log: FilteringBoundLogger) -> None:
    if not request.is_failure_callback:
        log.warning(
            "Task callback requested but is not a failure callback",
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
            ti_id=str(request.ti.id),
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
        log.info(
            "Executing Task callback at index %d: %s (ti_id=%s)",
            idx,
            callback_repr,
            request.ti.id,
        )
        try:
            callback(context)
        except Exception:
            log.exception(
                "Error in callback at index %d: %s (ti_id=%s)",
                idx,
                callback_repr,
                request.ti.id,
            )


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
class BaseDagFileProcessorProcess(WatchedSubprocess, LoggingMixin):
    """
    Parse one Dag file in a child process for the Dag processor manager.

    The child's output goes to the file's parse log, and its requests are answered with
    :attr:`client`. The parse is done once the child has exited and all its sockets are closed;
    :attr:`parsing_result` then holds what it sent. Subclasses start the child and send it the
    parse request.
    """

    logger_filehandle: BinaryIO | None = None
    parsing_result: DagFileParsingResult | None = None
    decoder: ClassVar[TypeAdapter[ToManager]] = TypeAdapter[ToManager](ToManager)
    had_callbacks: bool = False  # Track if this process was started with callbacks to prevent stale DAG detection false positives

    client: Client
    """The HTTP client to use for communication with the API server."""

    bundle_name: str
    dag_file_rel_path: str

    def _get_target_loggers(self) -> tuple[FilteringBoundLogger, ...]:
        base = super()._get_target_loggers()
        if not self.subprocess_logs_to_stdout:
            return base
        return tuple(
            logger.bind(dag_file=self.dag_file_rel_path, bundle_name=self.bundle_name) for logger in base
        )

    def _create_log_forwarder(
        self,
        loggers: tuple[FilteringBoundLogger, ...],
        name: str,
        *,
        data: bytes,
        log_level: int = logging.INFO,
    ) -> Callable[[socket], bool]:
        return super()._create_log_forwarder(
            loggers,
            name.replace("task.", "dag_processor.", 1),
            data=data,
            log_level=log_level,
        )

    def _handle_parsing_result(
        self, msg: DagFileParsingResult, log: FilteringBoundLogger, req_id: int
    ) -> RequestResult | ResponseSent:
        self.parsing_result = msg
        return None, {}

    _request_handlers: ClassVar[dict[type[BaseModel], RequestHandler[Any]]] = {
        **WatchedSubprocess._get_shared_request_handlers(
            DeleteVariable,
            GetConnection,
            GetPrevSuccessfulDagRun,
            GetPreviousDagRun,
            GetPreviousTI,
            GetTICount,
            GetTaskStates,
            GetVariable,
            GetVariableKeys,
            GetXCom,
            GetXComCount,
            GetXComSequenceItem,
            GetXComSequenceSlice,
            MaskSecret,
            PutVariable,
        ),
        **dict([register_request_method(DagFileParsingResult, _handle_parsing_result)]),
    }

    def _reject_request(self, msg, log: FilteringBoundLogger, req_id: int) -> None:
        log.error("Unhandled request", msg=msg)
        self.send_msg(
            None,
            request_id=req_id,
            error=ErrorResponse(detail={"status_code": 400, "message": "Unhandled request"}),
        )

    @property
    def is_ready(self) -> bool:
        if self._check_subprocess_exit() is None:
            # Process still alive, def can't be finished yet
            return False

        return not self._open_sockets

    def wait(self) -> int:
        raise NotImplementedError(f"Don't call wait on {type(self).__name__} objects")

    def close(self):
        self.cleanup_sockets_after_kill()
        if self.logger_filehandle is None:
            return
        try:
            self.logger_filehandle.close()
        except OSError:
            self.log.warning(
                "Failed to close log file handle for %s",
                self.dag_file_rel_path,
                exc_info=True,
            )


@attrs.define(kw_only=True)
class DagFileProcessorProcess(BaseDagFileProcessorProcess):
    """
    Parses dags with Task SDK API.

    This class provides a wrapper and management around a subprocess to parse a specific DAG file.

    Since DAGs are written with the Task SDK, we need to parse them in a task SDK process such that
    we can use the Task SDK definitions when serializing. This prevents potential conflicts with classes
    in core Airflow.
    """

    logger_filehandle: BinaryIO

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
        dag_file_rel_path: str,
        callbacks: list[CallbackRequest],
        known_artifacts: Sequence[TaskHandlerArtifact] = (),
        target: Callable[[], None] = _parse_file_entrypoint,
        client: Client,
        **kwargs,
    ) -> Self:
        logger = kwargs["logger"]

        # Parsing DAG files runs user code that can trigger macOS-unsafe ObjC
        # initialization (secret backends, connection/variable lookups, HTTP
        # clients). Fork+exec a clean interpreter there. Tests override `target`
        # with a stub to exercise the base infrastructure; keep bare fork for those.
        use_exec = target is _parse_file_entrypoint and supervisor._should_use_exec()

        # Pre-importing only helps the bare-fork child (it inherits the imports via
        # copy-on-write). An exec'd child re-imports from scratch, so skip it there
        # to avoid leaking user modules into the long-lived processor manager.
        if not use_exec:
            _pre_import_airflow_modules(os.fspath(path), logger)

        proc: Self = super().start(
            target=target,
            client=client,
            bundle_name=bundle_name,
            dag_file_rel_path=dag_file_rel_path,
            use_exec=use_exec,
            **kwargs,
        )
        proc.had_callbacks = bool(callbacks)  # Track if this process had callbacks
        proc._on_child_started(callbacks, path, bundle_path, bundle_name, known_artifacts=known_artifacts)
        return proc

    def _on_child_started(
        self,
        callbacks: list[CallbackRequest],
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
        *,
        known_artifacts: Sequence[TaskHandlerArtifact] = (),
    ) -> None:
        msg = DagFileParseRequest(
            file=os.fspath(path),
            bundle_path=bundle_path,
            bundle_name=bundle_name,
            callback_requests=callbacks,
            known_artifacts=list(known_artifacts),
        )
        self.send_msg(msg, request_id=0)
