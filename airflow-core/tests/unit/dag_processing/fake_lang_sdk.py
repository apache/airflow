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
"""A coordinator and Dag importer for ``.native`` Dag files, and a runtime to play in the parse child."""

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
    DagFileParseRequest,
    DagFileParsingResult,
    ToDagProcessor,
    ToManager,
)
from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.execution_time import supervisor
from airflow.sdk.execution_time.comms import CommsDecoder
from airflow.sdk.importers import DagSourceCode, reset_importer_registry

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence


@attrs.define(kw_only=True)
class FakeCoordinator(SubprocessCoordinator):
    """
    Parse ``.native`` files; the file's JSON names the command that parses it.

    ``argv`` is the command, ``schema_version`` its schema version, and ``command_error`` an error to
    raise instead.
    """

    def _build_parse_dag_command(self, *, path: Path) -> tuple[list[str], str | None]:
        spec = json.loads(path.read_text())
        if error := spec.get("command_error"):
            raise FileNotFoundError(error)
        return spec.get("argv", ["/bin/false"]), spec.get("schema_version")


class FakeCoordinatorDagImporter(CoordinatorDagImporter):
    coordinator_classpath = f"{__name__}.FakeCoordinator"
    artifact_suffix = ".native"
    supported_extensions = [".native"]

    def get_source_code(self, definition) -> DagSourceCode:
        return DagSourceCode(definition.read_text(), "fake")


@contextlib.contextmanager
def fake_coordinator(
    *keys: str,
    dag_bundle_to_coordinator: dict[str, str] | str | None = None,
    other_coordinators: dict[str, str] | None = None,
    **kwargs: Any,
) -> Iterator[None]:
    """
    Configure a ``FakeCoordinator`` for each of *keys* (``fake`` by default), and register its Dag importer.

    *dag_bundle_to_coordinator* is the ``[sdk] dag_bundle_to_coordinator`` option, as a dict or as the raw
    text. *other_coordinators* maps keys to the classpaths of coordinators of another class. The
    coordinators and the Dag importer registries are fresh inside and after the block. The parse child is
    a bare fork even on macOS, so it sees the test's ``parse_dag`` patch and config.
    """
    spec = {key: {"classpath": f"{__name__}.FakeCoordinator", "kwargs": kwargs} for key in keys or ("fake",)}
    spec.update({key: {"classpath": classpath} for key, classpath in (other_coordinators or {}).items()})
    config = {("sdk", "coordinators"): json.dumps(spec)}
    if dag_bundle_to_coordinator is not None:
        config[("sdk", "dag_bundle_to_coordinator")] = (
            dag_bundle_to_coordinator
            if isinstance(dag_bundle_to_coordinator, str)
            else json.dumps(dag_bundle_to_coordinator)
        )
    reset_importer_registry()
    try:
        with (
            conf_vars(config),
            mock.patch(
                "airflow.sdk.coordinators._dag_importer.COORDINATOR_DAG_IMPORTERS",
                (f"{__name__}.FakeCoordinatorDagImporter",),
            ),
            mock.patch.object(supervisor, "_should_use_exec", return_value=False),
        ):
            yield
    finally:
        reset_importer_registry()


def write_native_file(path: Path, **spec: Any) -> Path:
    path.write_text(json.dumps(spec))
    return path


def play_runtime(
    reply: Callable[[DagFileParseRequest, CommsDecoder], DagFileParsingResult | None],
    *,
    schema_version: str | None = None,
    log_lines: Sequence[dict[str, Any]] = (),
) -> Callable[..., None]:
    """
    Return a ``parse_dag`` that plays the runtime in the parse child instead of exec'ing one.

    It connects back as a runtime does, writes *log_lines* to its logs channel and answers the parse
    request with what *reply* returns; ``None`` sends no result.
    """

    def parse_dag(coordinator, *, comm_address, logs_address, report_schema_version, **kwargs) -> None:
        report_schema_version(schema_version)
        comm = socket.create_connection(comm_address)
        logs = socket.create_connection(logs_address)
        for line in log_lines:
            logs.sendall(json.dumps(line).encode() + b"\n")
        comms = CommsDecoder[ToDagProcessor, ToManager](socket=comm, body_decoder=TypeAdapter(ToDagProcessor))
        request = comms._get_response()
        assert isinstance(request, DagFileParseRequest)
        if (result := reply(request, comms)) is not None:
            comms.send(result)

    return parse_dag
