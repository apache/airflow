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

import contextlib
import os
import shutil
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING

import attrs

from airflow.configuration import conf
from airflow.providers.alibaba.cloud.hooks.oss import OSSHook
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI
    from airflow.utils.log.file_task_handler import LogMessages, LogSourceInfo


@attrs.define(kw_only=True)
class OSSRemoteLogIO(LoggingMixin):  # noqa: D101
    base_log_folder: Path = attrs.field(converter=Path)
    remote_base: str = ""
    delete_local_copy: bool = True

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
            # read log and remove old logs to get just the latest additions
            log = local_loc.read_text()
            has_uploaded = self.oss_write(log, remote_loc)
            if has_uploaded and self.delete_local_copy:
                shutil.rmtree(os.path.dirname(local_loc))

    @cached_property
    def base_folder(self):
        (_, base_folder) = OSSHook.parse_oss_url(self.remote_base)
        return base_folder

    @cached_property
    def bucket_name(self):
        (bucket_name, _) = OSSHook.parse_oss_url(self.remote_base)
        return bucket_name

    @cached_property
    def hook(self):
        remote_conn_id = conf.get("logging", "REMOTE_LOG_CONN_ID")
        self.log.info("remote_conn_id: %s", remote_conn_id)
        try:
            return OSSHook(oss_conn_id=remote_conn_id)
        except Exception as e:
            self.log.exception(e)
            self.log.error(
                'Could not create an OSSHook with connection id "%s". '
                "Please make sure that airflow[oss] is installed and "
                "the OSS connection exists.",
                remote_conn_id,
            )

    def read(self, relative_path, ti: RuntimeTI) -> tuple[LogSourceInfo, LogMessages | None]:
        logs: list[str] = []
        messages = [relative_path]

        if self.oss_log_exists(relative_path):
            logs.append(self.oss_read(relative_path, return_error=True))
            return messages, logs
        return messages, None

    def oss_log_exists(self, remote_log_location):
        """
        Check if remote_log_location exists in remote storage.

        :param remote_log_location: log's location in remote storage
        :return: True if location exists else False
        """
        oss_remote_log_location = f"{self.base_folder}/{remote_log_location}"
        with contextlib.suppress(Exception):
            return self.hook.key_exist(self.bucket_name, oss_remote_log_location)
        return False

    def oss_read(self, remote_log_location, return_error=False):
        """
        Return the log at the remote_log_location or '' if no logs are found or there is an error.

        :param remote_log_location: the log's location in remote storage
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise, returns '' when an error occurs.
        """
        oss_remote_log_location = f"{self.base_folder}/{remote_log_location}"
        try:
            self.log.info("read remote log: %s", oss_remote_log_location)
            return self.hook.read_key(self.bucket_name, oss_remote_log_location)
        except Exception:
            msg = f"Could not read logs from {oss_remote_log_location}"
            self.log.exception(msg)
            # return error if needed
            if return_error:
                return msg

    def oss_write(self, log, remote_log_location, append=True) -> bool:
        """
        Write the log to remote_log_location and return `True`; fails silently and returns `False` on error.

        :param log: the log to write to the remote_log_location
        :param remote_log_location: the log's location in remote storage
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :return: whether the log is successfully written to remote location or not.
        """
        oss_remote_log_location = f"{self.base_folder}/{remote_log_location}"
        pos = 0
        if append and self.oss_log_exists(remote_log_location):
            head = self.hook.head_key(self.bucket_name, oss_remote_log_location)
            pos = head.content_length
        self.log.info("log write pos is: %s", pos)
        try:
            self.log.info("writing remote log: %s", oss_remote_log_location)
            self.hook.append_string(self.bucket_name, log, oss_remote_log_location, pos)
        except Exception:
            self.log.exception(
                "Could not write logs to %s, log write pos is: %s, Append is %s",
                oss_remote_log_location,
                pos,
                append,
            )
            return False
        return True


class OSSTaskHandler(FileTaskHandler, LoggingMixin):
    """
    OSSTaskHandler is a python log handler that handles and reads task instance logs.

    Extends airflow FileTaskHandler and uploads to and reads from OSS remote storage.
    """

    def __init__(
        self,
        base_log_folder: str,
        oss_log_folder: str,
        max_bytes: int = 0,
        backup_count: int = 0,
        delay: bool = False,
        **kwargs,
    ) -> None:
        self.log.info("Using oss_task_handler for remote logging...")
        # support log file size handling of FileTaskHandler
        super().__init__(
            base_log_folder=base_log_folder, max_bytes=max_bytes, backup_count=backup_count, delay=delay
        )
        self.log_relative_path = ""
        self._hook = None
        self.closed = False
        self.upload_on_close = True
        self.delete_local_copy = kwargs.get(
            "delete_local_copy", conf.getboolean("logging", "delete_local_logs")
        )

        self.io = OSSRemoteLogIO(
            remote_base=oss_log_folder,
            base_log_folder=base_log_folder,
            delete_local_copy=kwargs.get(
                "delete_local_copy", conf.getboolean("logging", "delete_local_logs")
            ),
        )

    def set_context(self, ti):
        """Set the context of the handler."""
        super().set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to OSS remote storage.
        self.log_relative_path = self._render_filename(ti, ti.try_number)
        self.upload_on_close = not ti.raw

        self.ti = ti
        # Clear the file first so that duplicate data is not uploaded
        # when reusing the same path (e.g. with rescheduled sensors)
        if self.upload_on_close:
            with open(self.handler.baseFilename, "w"):
                pass

    def close(self):
        """Close and upload local log file to remote storage OSS."""
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

    def _read(self, ti, try_number, metadata=None):
        """
        Read logs of given task instance and try_number from OSS remote storage.

        If failed, read the log from task instance host machine.

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different from task instance passed in
        # set_context method.
        log_relative_path = self._render_filename(ti, try_number)
        remote_loc = log_relative_path

        if not self.oss_log_exists(remote_loc):
            return super()._read(ti, try_number, metadata)
        # If OSS remote file exists, we do not fetch logs from task instance
        # local machine even if there are errors reading remote logs, as
        # returned remote_log will contain error messages.
        remote_log = self.oss_read(remote_loc, return_error=True)
        log = f"*** Reading remote log from {remote_loc}.\n{remote_log}\n"
        return log, {"end_of_log": True}
