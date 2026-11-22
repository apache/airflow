# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://apache.org
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
from collections.abc import Generator
from pathlib import Path

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import (
    StructuredLogMessage,
    _interleave_logs,
    _read_local_logs_at_path,
    _read_remote_logs_at_path,
    _read_storage_logs,
)

_SAFE_PATH_COMPONENT = re.compile(r"[A-Za-z0-9._:+\-~@]+")


def validate_log_path_component(component: str) -> str:
    """Validate a single log path component, raising ValueError if it could escape the log folder."""
    if component in (".", "..") or not _SAFE_PATH_COMPONENT.fullmatch(component):
        raise ValueError(f"Invalid log path component: {component!r}")
    return component


def read_logs_at_path(relative_path: str) -> Generator[StructuredLogMessage, None, None]:
    """
    Stream logs not tied to a TaskInstance, trying remote storage first and then the local filesystem.

    The path is relative to the log folder. Callers build it and must validate any user-supplied
    parts with ``validate_log_path_component``. Deadline callbacks are the current user.
    """
    base_log_folder = os.path.realpath(conf.get("logging", "base_log_folder"))
    sources, remote_logs, local_logs = _read_storage_logs(
        read_remote=lambda: _read_remote_logs_at_path(relative_path),
        read_local=lambda: _read_local_logs_at_path(base_log_folder, Path(base_log_folder, relative_path)),
        check_local_when_remote_exists=False,
        suppress_remote_errors=(Exception,),
    )
    log_streams = [*remote_logs, *local_logs]

    if not log_streams:
        yield StructuredLogMessage(event="No logs found.")
        return

    yield StructuredLogMessage(event="::group::Log message source details", sources=sources)  # type: ignore[call-arg]
    yield StructuredLogMessage(event="::endgroup::")
    yield from _interleave_logs(*log_streams)
