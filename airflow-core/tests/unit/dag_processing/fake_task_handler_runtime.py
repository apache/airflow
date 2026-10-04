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
from airflow.sdk.execution_time.coordinator import reset_coordinator_manager

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence


@attrs.define(kw_only=True)
class FakeCoordinator(SubprocessCoordinator):
    """
    The artifact's JSON names the command that parses it.

    ``argv`` is the command, ``schema_version`` its schema version, and ``command_error`` an error to
    raise instead.
    """

    def _build_parse_task_handler_command(self, *, path: Path) -> tuple[list[str], str | None]:
        spec = json.loads(path.read_text())
        if error := spec.get("command_error"):
            raise FileNotFoundError(error)
        return spec.get("argv", ["/bin/false"]), spec.get("schema_version")


@contextlib.contextmanager
def fake_coordinator(*, other_coordinators: dict[str, str] | None = None, **kwargs: Any) -> Iterator[None]:
    """
    Configure a ``FakeCoordinator`` as ``fake``, with fresh coordinators inside and after the block.

    *other_coordinators* maps keys to the classpaths of coordinators of another class. The parse child is
    a bare fork even on macOS, so it sees the test's patches and config.
    """
    spec = {"fake": {"classpath": f"{__name__}.FakeCoordinator", "kwargs": kwargs}}
    spec.update({key: {"classpath": classpath} for key, classpath in (other_coordinators or {}).items()})
    reset_coordinator_manager()
    try:
        with (
            conf_vars({("sdk", "coordinators"): json.dumps(spec)}),
            mock.patch.object(supervisor, "_should_use_exec", return_value=False),
        ):
            yield
    finally:
        reset_coordinator_manager()


def write_artifact(path: Path, **spec: Any) -> Path:
    path.write_text(json.dumps(spec))
    return path


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
