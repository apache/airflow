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
"""Reader for callback execution logs stored in remote or local storage."""

from __future__ import annotations

import os
import re
from collections.abc import Generator
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import (
    StructuredLogMessage,
    _get_compatible_log_stream,
    _interleave_logs,
    _stream_lines_by_chunk,
)

if TYPE_CHECKING:
    from airflow._shared.logging.remote import LogSourceInfo, RawLogStream, StreamingLogResponse

_SAFE_PATH_COMPONENT = re.compile(r"[A-Za-z0-9._:+\-~@]+")


def validate_log_path_component(component: str) -> str:
    """Validate a single log path component, raising ValueError if it could escape the log folder."""
    if component in (".", "..") or not _SAFE_PATH_COMPONENT.fullmatch(component):
        raise ValueError(f"Invalid log path component: {component!r}")
    return component


def _get_callback_log_relative_paths(dag_id: str, run_id: str, callback_id: str) -> list[str]:
    """
    Construct the relative log paths for a callback execution.

    The executor path matches the format used in ExecuteCallback.make():
        executor_callbacks/{dag_id}/{run_id}/{callback_id}
    The triggerer path matches what TriggerLoggingFactory writes for callback triggers:
        triggerer_callbacks/{dag_id}/{run_id}/{callback_id}
    """
    for component in (dag_id, run_id, callback_id):
        validate_log_path_component(component)
    return [
        f"executor_callbacks/{dag_id}/{run_id}/{callback_id}",
        f"triggerer_callbacks/{dag_id}/{run_id}/{callback_id}",
    ]


def read_callback_log(
    dag_id: str,
    run_id: str,
    callback_id: str,
) -> Generator[StructuredLogMessage, None, None]:
    """
    Stream callback logs from remote and/or local storage.

    Tries both executor_callbacks and triggerer_callbacks paths. For each path, tries
    remote storage first (if configured), then falls back to the local filesystem.

    :param dag_id: The Dag ID associated with the callback.
    :param run_id: The Dag run ID associated with the callback.
    :param callback_id: The unique callback identifier.
    :return: Generator of StructuredLogMessage objects.
    """
    relative_paths = _get_callback_log_relative_paths(dag_id, run_id, callback_id)

    sources: LogSourceInfo = []
    log_streams: list[RawLogStream] = []

    for relative_path in relative_paths:
        with suppress(Exception):
            remote_sources, remote_log_streams = _read_callback_remote_logs(relative_path)
            sources.extend(remote_sources)
            log_streams.extend(remote_log_streams)

        if not log_streams:
            local_sources, local_log_streams = _read_callback_local_logs(relative_path)
            sources.extend(local_sources)
            log_streams.extend(local_log_streams)

        # If we found logs at this path, no need to check the next path
        if log_streams:
            break

    if not log_streams:
        yield StructuredLogMessage(event="No callback logs found.")
        return

    yield StructuredLogMessage(event="::group::Log message source details", sources=sources)  # type: ignore[call-arg]
    yield StructuredLogMessage(event="::endgroup::")
    yield from _interleave_logs(*log_streams)


def _read_callback_remote_logs(relative_path: str) -> StreamingLogResponse:
    """Read callback logs from the configured remote log storage."""
    from airflow.logging_config import get_remote_task_log

    remote_io = get_remote_task_log()
    if remote_io is None:
        return [], []

    # Callbacks have no TaskInstance, so pass ti=None; remote handlers only use it
    # for optional metadata (e.g. CloudWatch end_date) and read by relative path.
    if stream_method := getattr(remote_io, "stream", None):
        sources, logs = stream_method(relative_path, None)
        return sources, logs or []

    sources, logs = remote_io.read(relative_path, None)  # type: ignore[arg-type]
    if not logs:
        return sources, []

    return sources, [_get_compatible_log_stream(logs)]


def _read_callback_local_logs(relative_path: str) -> StreamingLogResponse:
    """Read callback logs from the local filesystem."""
    base_log_folder = os.path.realpath(conf.get("logging", "base_log_folder"))
    log_path = Path(base_log_folder, *(validate_log_path_component(p) for p in relative_path.split("/")))

    sources: list[str] = []
    log_streams: list[RawLogStream] = []

    for path in sorted(log_path.parent.glob(log_path.name + "*")):
        # Containment check (defense in depth, e.g. against symlinks escaping the log folder).
        resolved_path = os.path.realpath(path)
        try:
            if os.path.commonpath([base_log_folder, resolved_path]) != base_log_folder:
                continue
        except ValueError:
            continue

        try:
            log_stream = _stream_lines_by_chunk(open(resolved_path, encoding="utf-8"))
        except OSError:
            continue
        sources.append(os.fspath(path))
        log_streams.append(log_stream)

    return sources, log_streams
