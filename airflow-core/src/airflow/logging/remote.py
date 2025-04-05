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
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import structlog.typing

    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI
    from airflow.utils.log.file_task_handler import LogMessages, LogSourceInfo


class RemoteLogIO(Protocol):
    """Interface for remote task loggers."""

    @property
    def processors(self) -> tuple[structlog.typing.Processor, ...]: ...

    """
    List of structlog processors to install in the task write path.

    This is useful if a remote logging provider wants to either transform the structured log messages as they
    are being written to a file, or if you want to upload messages as they are generated.
    """

    def upload(self, path: os.PathLike | str, ti: RuntimeTI) -> None:
        """Upload the given log path to the remote storage."""
        ...

    def read(self, relative_path: str, ti: RuntimeTI) -> tuple[LogSourceInfo, LogMessages | None]:
        """Read logs from the given remote log path."""
        ...
