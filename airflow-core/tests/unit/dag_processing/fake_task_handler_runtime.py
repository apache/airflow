#
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
"""A coordinator whose artifacts name the command that parses them, and a runtime a test can play."""

from __future__ import annotations

import contextlib
import json
import os
import socket
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest import mock

import attrs
from pydantic import TypeAdapter

from airflow.dag_processing.processor import (
    TaskHandlerParseRequest,
    TaskHandlerParsingResult,
    ToManager,
    ToSDKTaskHandlerProcessor,
)
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.execution_time import supervisor
from airflow.sdk.execution_time.comms import CommsDecoder
from airflow.sdk.execution_time.coordinator import TaskHandlerCandidate, reset_coordinator_manager

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence


@attrs.define(kw_only=True)
class FakeCoordinator(SubprocessCoordinator):
    """
    The JSON of an ``*.artifact`` file names the command that parses it.

    ``argv`` is the command, ``schema_version`` its schema version, and ``command_error`` an error to
    raise instead. The listing reads the stored ``cache_digest``, and ``listing_error`` as why the
    artifact cannot be probed. ``task_handlers`` is the answer :func:`reply_with_task_handlers` sends.
    """

    def _build_parse_task_handler_command(self, *, path: Path) -> tuple[list[str], str | None]:
        spec = json.loads(path.read_text())
        if error := spec.get("command_error"):
            raise FileNotFoundError(error)
        return spec.get("argv", ["/bin/false"]), spec.get("schema_version")

    def _read_task_handler_candidate(self, path: Path, *, rel_path: str) -> TaskHandlerCandidate | None:
        if path.suffix != ".artifact":
            return None
        content = path.read_bytes()
        spec = json.loads(content)
        return TaskHandlerCandidate(
            rel_path=rel_path,
            size_bytes=len(content),
            cache_digest=spec.get("cache_digest"),
            error=spec.get("listing_error"),
        )


@contextlib.contextmanager
def fake_coordinator(**kwargs: Any) -> Iterator[None]:
    """
    Configure a ``FakeCoordinator`` as ``fake``, with fresh coordinators inside and after the block.

    The parse child is a bare fork even on macOS, so it sees the test's patches and config.
    """
    spec = {"fake": {"classpath": f"{__name__}.FakeCoordinator", "kwargs": kwargs}}
    reset_coordinator_manager()
    try:
        with (
            conf_vars({("sdk", "coordinators"): json.dumps(spec)}),
            mock.patch.object(supervisor, "_should_use_exec", return_value=False),
        ):
            yield
    finally:
        reset_coordinator_manager()


FAKE_COORDINATOR = f"{__name__}.FakeCoordinator"
LOCAL_BUNDLE = "airflow.dag_processing.bundles.local.LocalDagBundle"


@contextlib.contextmanager
def task_handler_config(
    dag_bundle: Path,
    artifacts: Path,
    coordinators: dict[str, Any] | None = None,
    *,
    queue_to_coordinator: dict[str, str] | None = None,
    bundles: list[dict[str, Any]] | None = None,
    multi_team: bool = False,
) -> Iterator[None]:
    """
    Route the queue ``fake-queue`` to a ``FakeCoordinator`` reading the ``task-handlers`` Dag bundle.

    By default *dag_bundle* is the Dag bundle ``dags`` and *artifacts* is ``task-handlers``; the other
    arguments replace those parts of the configuration. Coordinators are fresh inside and after the block.
    """
    if coordinators is None:
        coordinators = {
            "fake": {"classpath": FAKE_COORDINATOR, "kwargs": {"task_handler_bundle_name": "task-handlers"}}
        }
    if bundles is None:
        bundles = [
            {"name": "dags", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(dag_bundle)}},
            {"name": "task-handlers", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(artifacts)}},
        ]
    reset_coordinator_manager()
    try:
        with conf_vars(
            {
                ("core", "load_examples"): "False",
                ("core", "multi_team"): str(multi_team),
                ("dag_processor", "dag_bundle_config_list"): json.dumps(bundles),
                ("sdk", "coordinators"): json.dumps(coordinators),
                ("sdk", "queue_to_coordinator"): json.dumps(queue_to_coordinator or {"fake-queue": "fake"}),
            }
        ):
            yield
    finally:
        reset_coordinator_manager()


def write_artifact(path: Path, **spec: Any) -> Path:
    path.write_text(json.dumps(spec))
    return path


def reply_with_task_handlers(
    request: TaskHandlerParseRequest, comms: CommsDecoder | None
) -> TaskHandlerParsingResult:
    """Answer with the ``task_handlers`` of the artifact's JSON, as declarations in their wire form."""
    spec = json.loads(Path(request.file).read_text())
    return TaskHandlerParsingResult.model_validate(
        {"fileloc": request.file, "task_handlers": spec.get("task_handlers", {})}
    )


def play_runtime(
    reply: Callable[[TaskHandlerParseRequest, CommsDecoder], TaskHandlerParsingResult | None],
    *,
    schema_version: str | None = None,
    log_lines: Sequence[dict[str, Any]] = (),
) -> Callable[..., None]:
    """
    Return a ``parse_task_handler`` that plays the runtime in the parse child instead of exec'ing one.

    It connects back as a runtime does, writes *log_lines* to its logs channel and answers the parse
    request with what *reply* returns; ``None`` sends no result.
    """

    def parse_task_handler(
        coordinator, *, comm_address, logs_address, report_schema_version, **kwargs
    ) -> None:
        report_schema_version(schema_version)
        with (
            socket.create_connection(comm_address) as comm,
            socket.create_connection(logs_address) as logs,
        ):
            for line in log_lines:
                logs.sendall(json.dumps(line).encode() + b"\n")
            comms = CommsDecoder[ToSDKTaskHandlerProcessor, ToManager](
                socket=comm, body_decoder=TypeAdapter(ToSDKTaskHandlerProcessor)
            )
            request = comms._get_response()
            assert isinstance(request, TaskHandlerParseRequest)
            if (result := reply(request, comms)) is not None:
                comms.send(result)

    return parse_task_handler
