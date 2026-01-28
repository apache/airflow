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
from collections.abc import Callable, Generator
from importlib import import_module
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, runtime_checkable

import structlog

LogMessages: TypeAlias = list[str]
"""The legacy format of log messages before 3.0.4"""
LogSourceInfo: TypeAlias = list[str]
"""Information about the log fetching process for display to a user"""
RawLogStream: TypeAlias = Generator[str, None, None]
"""Raw log stream, containing unparsed log lines"""
LogResponse: TypeAlias = tuple[LogSourceInfo, LogMessages | None]
"""Legacy log response, containing source information and log messages"""
StreamingLogResponse: TypeAlias = tuple[LogSourceInfo, list[RawLogStream]]
"""Streaming log response, containing source information, stream of log lines"""

if TYPE_CHECKING:
    # RuntimeTI is only needed for type checking to avoid circular imports
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI

log = structlog.getLogger(__name__)


class RemoteLogIO(Protocol):
    """Interface for remote task loggers."""

    @property
    def processors(self) -> tuple[structlog.typing.Processor, ...]:
        """
        List of structlog processors to install in the task write path.

        This is useful if a remote logging provider wants to either transform
        the structured log messages as they are being written to a file, or if
        you want to upload messages as they are generated.
        """
        ...

    def upload(self, path: os.PathLike | str, ti: RuntimeTI) -> None:
        """Upload the given log path to the remote storage."""
        ...

    def read(self, relative_path: str, ti: RuntimeTI) -> LogResponse:
        """Read logs from the given remote log path."""
        ...


@runtime_checkable
class RemoteLogStreamIO(RemoteLogIO, Protocol):
    """Interface for remote task loggers with stream-based read support."""

    def stream(self, relative_path: str, ti: RuntimeTI) -> StreamingLogResponse:
        """Stream-based read interface for reading logs from the given remote log path."""
        ...


def discover_remote_log_handler(
    logging_class_path: str,
    fallback_path: str,
    import_string: Callable[[str], Any],
) -> tuple[RemoteLogIO | None, str | None]:
    """Discover and load the remote log handler from a logging config module."""
    # Sometimes we end up with `""` as the value!
    logging_class_path = logging_class_path or fallback_path

    try:
        logging_config = import_string(logging_class_path)

        # Make sure that the variable is in scope
        if not isinstance(logging_config, dict):
            return None, None

        modpath = logging_class_path.rsplit(".", 1)[0]
        mod = import_module(modpath)

        # Load remote logging configuration from the custom module
        remote_task_log = getattr(mod, "REMOTE_TASK_LOG", None)
        default_remote_conn_id = getattr(mod, "DEFAULT_REMOTE_CONN_ID", None)

        return remote_task_log, default_remote_conn_id

    except Exception as err:
        log.info("Remote task logs will not be available due to an error: %s", err)
        return None, None
