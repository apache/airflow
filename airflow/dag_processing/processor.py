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

import os
import sys
import traceback
from collections.abc import Generator
from typing import TYPE_CHECKING, Annotated, Callable, Literal, Union

import attrs
from pydantic import BaseModel, Field, TypeAdapter

from airflow.callbacks.callback_requests import (
    CallbackRequest,
    DagCallbackRequest,
    TaskCallbackRequest,
)
from airflow.configuration import conf
from airflow.models.dagbag import DagBag
from airflow.sdk.execution_time.comms import GetConnection, GetVariable
from airflow.sdk.execution_time.supervisor import WatchedSubprocess
from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedDAG
from airflow.stats import Stats

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.typing_compat import Self
    from airflow.utils.context import Context


def _parse_file_entrypoint():
    import os

    import structlog

    from airflow.sdk.execution_time import task_runner
    # Parse DAG file, send JSON back up!

    comms_decoder = task_runner.CommsDecoder[DagFileParseRequest, DagFileParsingResult](
        input=sys.stdin,
        decoder=TypeAdapter[DagFileParseRequest](DagFileParseRequest),
    )
    msg = comms_decoder.get_message()
    comms_decoder.request_socket = os.fdopen(msg.requests_fd, "wb", buffering=0)

    log = structlog.get_logger(logger_name="task")

    result = _parse_file(msg, log)
    comms_decoder.send_request(log, result)


def _parse_file(msg: DagFileParseRequest, log: FilteringBoundLogger) -> DagFileParsingResult:
    # TODO: Set known_pool names on DagBag!
    bag = DagBag(
        dag_folder=msg.file,
        include_examples=False,
        safe_mode=True,
        load_op_links=False,
    )
    serialized_dags, serialization_import_errors = _serialize_dags(bag, log)
    bag.import_errors.update(serialization_import_errors)
    dags = [LazyDeserializedDAG(data=serdag) for serdag in serialized_dags]
    result = DagFileParsingResult(
        fileloc=msg.file,
        serialized_dags=dags,
        import_errors=bag.import_errors,
        # TODO: Make `bag.dag_warnings` not return SQLA model objects
        warnings=[],
    )

    if msg.callback_requests:
        _execute_callbacks(bag, msg.callback_requests, log)
    return result


def _serialize_dags(bag: DagBag, log: FilteringBoundLogger) -> tuple[list[dict], dict[str, str]]:
    serialization_import_errors = {}
    serialized_dags = []
    for dag in bag.dags.values():
        try:
            serialized_dag = SerializedDAG.to_dict(dag)
            serialized_dags.append(serialized_dag)
        except Exception:
            log.exception("Failed to serialize DAG: %s", dag.fileloc)
            dagbag_import_error_traceback_depth = conf.getint(
                "core", "dagbag_import_error_traceback_depth", fallback=None
            )
            serialization_import_errors[dag.fileloc] = traceback.format_exc(
                limit=-dagbag_import_error_traceback_depth
            )
    return serialized_dags, serialization_import_errors


def _execute_callbacks(
    dagbag: DagBag, callback_requests: list[CallbackRequest], log: FilteringBoundLogger
) -> None:
    for request in callback_requests:
        log.debug("Processing Callback Request", request=request.to_json())
        if isinstance(request, TaskCallbackRequest):
            raise NotImplementedError(
                "Haven't coded Task callback yet - https://github.com/apache/airflow/issues/44354!"
            )
            # _execute_task_callbacks(dagbag, request)
        elif isinstance(request, DagCallbackRequest):
            _execute_dag_callbacks(dagbag, request, log)


def _execute_dag_callbacks(dagbag: DagBag, request: DagCallbackRequest, log: FilteringBoundLogger) -> None:
    dag = dagbag.dags[request.dag_id]

    callbacks = dag.on_failure_callback if request.is_failure_callback else dag.on_success_callback
    if not callbacks:
        log.warning("Callback requested, but dag didn't have any", dag_id=request.dag_id)
        return

    callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
    # TODO:We need a proper context object!
    context: Context = {}  # type: ignore[assignment]

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


class DagFileParseRequest(BaseModel):
    """
    Request for DAG File Parsing.

    This is the request that the manager will send to the DAG parser with the dag file and
    any other necessary metadata.
    """

    file: str
    requests_fd: int
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


ToParent = Annotated[
    Union[DagFileParsingResult, GetConnection, GetVariable],
    Field(discriminator="type"),
]


@attrs.define()
class DagFileProcessorProcess(WatchedSubprocess):
    """
    Parses dags with Task SDK API.

    This class provides a wrapper and management around a subprocess to parse a specific DAG file.

    Since DAGs are written with the Task SDK, we need to parse them in a task SDK process such that
    we can use the Task SDK definitions when serializing. This prevents potential conflicts with classes
    in core Airflow.
    """

    parsing_result: DagFileParsingResult | None = None

    @classmethod
    def start(  # type: ignore[override]
        cls,
        path: str | os.PathLike[str],
        callbacks: list[CallbackRequest],
        target: Callable[[], None] = _parse_file_entrypoint,
        **kwargs,
    ) -> Self:
        return super().start(path, callbacks, target=target, client=None, **kwargs)  # type:ignore[arg-type]

    def _on_child_started(  # type: ignore[override]
        self, callbacks: list[CallbackRequest], path: str | os.PathLike[str], child_comms_fd: int
    ) -> None:
        msg = DagFileParseRequest(
            file=os.fspath(path),
            requests_fd=child_comms_fd,
            callback_requests=callbacks,
        )
        self.stdin.write(msg.model_dump_json().encode() + b"\n")

    def handle_requests(self, log: FilteringBoundLogger) -> Generator[None, bytes, None]:
        # TODO: Make decoder an instance variable, then this can live in the base class
        decoder = TypeAdapter[ToParent](ToParent)

        while True:
            line = yield

            try:
                msg = decoder.validate_json(line)
            except Exception:
                log.exception("Unable to decode message", line=line)
                continue

            self._handle_request(msg, log)  # type: ignore[arg-type]

    def _handle_request(self, msg: ToParent, log: FilteringBoundLogger) -> None:  # type: ignore[override]
        if isinstance(msg, DagFileParsingResult):
            self.parsing_result = msg
            return
        # GetVariable etc -- parsing a dag can run top level code that asks for an Airflow Variable
        super()._handle_request(msg, log)

    @property
    def is_ready(self) -> bool:
        if self._check_subprocess_exit() is None:
            # Process still alive, def can't be finished yet
            return False

        return self._num_open_sockets == 0

    @property
    def start_time(self) -> float:
        return self._process.create_time()

    def wait(self) -> int:
        raise NotImplementedError(f"Don't call wait on {type(self).__name__} objects")
