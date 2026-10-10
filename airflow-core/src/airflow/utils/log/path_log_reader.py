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
"""Reader for logs not tied to a TaskInstance, looked up by their path relative to the log folder."""

from __future__ import annotations

import os
import re
from collections.abc import Generator, Iterable
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import (
    FileTaskHandler,
    StructuredLogMessage,
    _get_compatible_log_stream,
    _interleave_logs,
)

if TYPE_CHECKING:
    from airflow._shared.logging.remote import LogSourceInfo, RawLogStream, StreamingLogResponse

_SAFE_PATH_COMPONENT = re.compile(r"[A-Za-z0-9._:+\-~@]+")


def validate_log_path_component(component: str) -> str:
    """Validate a single log path component, raising ValueError if it could escape the log folder."""
    if component in (".", "..") or not _SAFE_PATH_COMPONENT.fullmatch(component):
        raise ValueError(f"Invalid log path component: {component!r}")
    return component


def read_logs_at_paths(relative_paths: Iterable[str]) -> Generator[StructuredLogMessage, None, None]:
    """
    Stream logs not tied to a TaskInstance, trying remote storage first and then the local filesystem.

    Each path is relative to the log folder and is tried in order; the first one with logs wins.
    Callers build the paths and must validate any user-supplied parts with
    ``validate_log_path_component``. Deadline callbacks are the current user.
    """
    sources: LogSourceInfo = []
    log_streams: list[RawLogStream] = []

    for relative_path in relative_paths:
        with suppress(Exception):
            remote_sources, remote_log_streams = _read_remote_logs(relative_path)
            sources.extend(remote_sources)
            log_streams.extend(remote_log_streams)

        if not log_streams:
            local_sources, local_log_streams = _read_local_logs(relative_path)
            sources.extend(local_sources)
            log_streams.extend(local_log_streams)

        if log_streams:
            break

    if not log_streams:
        yield StructuredLogMessage(event="No logs found.")
        return

    yield StructuredLogMessage(event="::group::Log message source details", sources=sources)  # type: ignore[call-arg]
    yield StructuredLogMessage(event="::endgroup::")
    yield from _interleave_logs(*log_streams)


def _read_remote_logs(relative_path: str) -> StreamingLogResponse:
    from airflow.logging_config import get_remote_task_log

    remote_io = get_remote_task_log()
    if remote_io is None:
        return [], []

    # Prefer .stream(), which yields the remote log lazily, over .read(), which loads it all into memory.
    # These logs have no TaskInstance, so ti is None.
    if stream_method := getattr(remote_io, "stream", None):
        sources, logs = stream_method(relative_path, None)
        return sources, logs or []

    sources, logs = remote_io.read(relative_path, None)  # type: ignore[arg-type]
    if not logs:
        return sources, []

    return sources, [_get_compatible_log_stream(logs)]


def _read_local_logs(relative_path: str) -> StreamingLogResponse:
    """Read with the task handler's symlink-safe local reader."""
    base_log_folder = os.path.realpath(conf.get("logging", "base_log_folder"))
    return FileTaskHandler(base_log_folder)._read_from_local(Path(base_log_folder, relative_path))
