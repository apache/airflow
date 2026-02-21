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
from __future__ import annotations

import logging
import os
import shutil
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import attrs

from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.common.compat.sdk import conf
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI
    from airflow.utils.log.file_task_handler import LogMessages, LogSourceInfo


@attrs.define(kw_only=True)
class HdfsRemoteLogIO(LoggingMixin):  # noqa: D101
    remote_base: str
    base_log_folder: Path = attrs.field(converter=Path)
    delete_local_copy: bool

    processors = ()

    def upload(self, path: os.PathLike | str, ti: RuntimeTI):
        """Upload the given log path to the remote storage."""
        path = Path(path)
        if path.is_absolute():
            local_loc = path
            remote_loc = os.path.join(self.remote_base, path.relative_to(self.base_log_folder))
        else:
            local_loc = self.base_log_folder.joinpath(path)
            remote_loc = os.path.join(self.remote_base, path)

        if local_loc.is_file():
            self.hook.load_file(local_loc, remote_loc)
            if self.delete_local_copy:
                shutil.rmtree(os.path.dirname(local_loc))

    @cached_property
    def hook(self):
        """Returns WebHDFSHook."""
        return WebHDFSHook(webhdfs_conn_id=conf.get("logging", "REMOTE_LOG_CONN_ID"))

    def read(self, relative_path: str, ti: RuntimeTI) -> tuple[LogSourceInfo, LogMessages]:
        logs = []
        messages = []
        file_path = os.path.join(self.remote_base, relative_path)
        if self.hook.check_for_path(file_path):
            logs.append(self.hook.read_file(file_path).decode("utf-8"))
        else:
            messages.append(f"No logs found on hdfs for ti={ti}")
        return messages, logs


class HdfsTaskHandler(FileTaskHandler, LoggingMixin):
    """
    HdfsTaskHandler is a Python logging handler that handles and reads task instance logs.

    It extends airflow FileTaskHandler and uploads to and reads from HDFS.
    """

    def __init__(
        self,
        base_log_folder: str,
        hdfs_log_folder: str,
        max_bytes: int = 0,
        backup_count: int = 0,
        delay: bool = False,
        **kwargs,
    ) -> None:
        # support log file size handling of FileTaskHandler
        super().__init__(
            base_log_folder=base_log_folder, max_bytes=max_bytes, backup_count=backup_count, delay=delay
        )
        self.handler: logging.FileHandler | None = None
        self.remote_base = urlsplit(hdfs_log_folder).path
        self.log_relative_path = ""
        self._hook = None
        self.closed = False
        self.upload_on_close = True

        self.io = HdfsRemoteLogIO(
            remote_base=self.remote_base,
            base_log_folder=base_log_folder,
            delete_local_copy=kwargs.get(
                "delete_local_copy", conf.getboolean("logging", "delete_local_logs")
            ),
        )

    def set_context(self, ti: TaskInstance, *, identifier: str | None = None) -> None:
        super().set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to HDFS storage.
        if TYPE_CHECKING:
            assert self.handler is not None

        full_path = self.handler.baseFilename
        self.log_relative_path = Path(full_path).relative_to(self.local_base).as_posix()
        is_trigger_log_context = getattr(ti, "is_trigger_log_context", False)
        self.upload_on_close = is_trigger_log_context or not ti.raw
        self.ti = ti
        # Clear the file first so that duplicate data is not uploaded
        # when reusing the same path (e.g. with rescheduled sensors)
        if self.upload_on_close:
            with open(self.handler.baseFilename, "w"):
                pass

    def close(self):
        """Close and upload local log file to HDFS."""
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super().close()

        if not self.upload_on_close:
            return

        if hasattr(self, "ti"):
            self.io.upload(self.log_relative_path, self.ti)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read_remote_logs(self, ti, try_number, metadata=None) -> tuple[LogSourceInfo, LogMessages]:
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different from task instance passed
        # in set_context method.
        worker_log_rel_path = self._render_filename(ti, try_number)
        messages, logs = self.io.read(worker_log_rel_path, ti)
        return messages, logs
