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

import logging
import os
from collections.abc import Generator
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import (
    StructuredLogMessage,
    _interleave_logs,
    _stream_lines_by_chunk,
)

if TYPE_CHECKING:
    from airflow._shared.logging.remote import LogSourceInfo, RawLogStream

logger = logging.getLogger(__name__)


def _get_callback_log_relative_path(dag_id: str, run_id: str, callback_id: str) -> str:
    """
    Construct the relative log path for a callback execution.

    This must match the path format used in ExecuteCallback.make():
        executor_callbacks/{dag_id}/{run_id}/{callback_id}
    """
    return f"executor_callbacks/{dag_id}/{run_id}/{callback_id}"


def read_callback_log(
    dag_id: str,
    run_id: str,
    callback_id: str,
) -> Generator[StructuredLogMessage, None, None]:
    """
    Read callback logs from remote and/or local storage.

    Tries remote storage first (if configured), then falls back to local filesystem.
    Returns a generator of StructuredLogMessage objects suitable for the API response.

    :param dag_id: The Dag ID associated with the callback.
    :param run_id: The Dag run ID associated with the callback.
    :param callback_id: The unique callback identifier.
    :return: Generator of StructuredLogMessage objects.
    """
    relative_path = _get_callback_log_relative_path(dag_id, run_id, callback_id)

    sources: LogSourceInfo = []
    remote_logs: list[RawLogStream] = []
    local_logs: list[RawLogStream] = []

    # Try remote storage first
    with suppress(Exception):
        remote_sources, remote_log_streams = _read_callback_remote_logs(relative_path)
        if remote_log_streams:
            sources.extend(remote_sources)
            remote_logs.extend(remote_log_streams)

    # Try local filesystem
    if not remote_logs:
        local_sources, local_log_streams = _read_callback_local_logs(relative_path)
        if local_log_streams:
            sources.extend(local_sources)
            local_logs.extend(local_log_streams)

    if not remote_logs and not local_logs:
        yield StructuredLogMessage(event="No callback logs found.", timestamp=None)
        return

    # Emit source information header
    yield StructuredLogMessage(event="::group::Log message source details", sources=sources)  # type: ignore[call-arg]
    yield StructuredLogMessage(event="::endgroup::")

    # Interleave and yield all log streams
    log_stream = _interleave_logs(*remote_logs, *local_logs)
    yield from log_stream


def _read_callback_remote_logs(
    relative_path: str,
) -> tuple[list[str], list[RawLogStream]]:
    """Read callback logs from the configured remote log storage."""
    from airflow.logging_config import get_remote_task_log

    remote_io = get_remote_task_log()
    if remote_io is None:
        return [], []

    # RemoteLogIO.read() takes (relative_path, ti) -- for S3 the ti is not used,
    # for CloudWatch it uses ti.end_date (with getattr fallback to None).
    # We pass None since callbacks don't have a TaskInstance.
    if stream_method := getattr(remote_io, "stream", None):
        sources, logs = stream_method(relative_path, None)
        return sources, logs or []

    sources, logs = remote_io.read(relative_path, None)  # type: ignore[arg-type]
    if not logs:
        return sources, []

    # Convert legacy string logs to stream format
    from airflow.utils.log.file_task_handler import _get_compatible_log_stream

    return sources, [_get_compatible_log_stream(logs)]


def _validate_path_component(component: str) -> str:
    """Validate and return a path component, raising ValueError if unsafe."""
    import re

    if component in (".", "..") or not re.fullmatch(r"[A-Za-z0-9._:+\-~@]+", component):
        raise ValueError(f"Invalid path component: {component!r}")
    return component


def _read_callback_local_logs(
    relative_path: str,
) -> tuple[list[str], list[RawLogStream]]:
    """Read callback logs from the local filesystem."""
    base_log_folder = os.path.realpath(conf.get("logging", "base_log_folder"))

    parts = relative_path.split("/")
    safe_parts = [_validate_path_component(p) for p in parts if p]
    safe_relative = os.path.join(*safe_parts)

    log_path = Path(base_log_folder) / safe_relative

    sources: list[str] = []
    log_streams: list[RawLogStream] = []

    paths = sorted(log_path.parent.glob(log_path.name + "*"))
    if not paths:
        return sources, log_streams

    for path in paths:
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
