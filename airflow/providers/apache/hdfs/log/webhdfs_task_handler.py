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
import pathlib
from functools import cached_property
from typing import Any

from airflow.configuration import conf
from airflow.models import TaskInstance
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class WebHDFSTaskHandler(FileTaskHandler, LoggingMixin):
    """A TaskHandler that uploads logs to HDFS once done. During the run, it will show the executor logs."""

    trigger_should_wrap = True

    def __init__(
        self,
        base_log_folder: str,
        webhdfs_log_folder: str,
        filename_template: str | None = None,
    ):
        super().__init__(base_log_folder=base_log_folder, filename_template=filename_template)
        self.remote_base = WebHDFSHook.parse_webhdfs_url(webhdfs_log_folder)
        self.log_relative_path = ""
        self.upload_on_close = False
        self.closed = False

    @cached_property
    def hook(self) -> WebHDFSHook:
        """Returns WebHDFSHook."""
        return WebHDFSHook(webhdfs_conn_id=conf.get("logging", "REMOTE_LOG_CONN_ID"))

    def set_context(self, ti: TaskInstance) -> None:
        """Provide task_instance context to airflow task handler."""
        super().set_context(ti)

        # Local location and remote location is needed to open and
        # upload local log file to S3 remote storage.
        full_path = self.handler.baseFilename  # type: ignore[union-attr]
        self.log_relative_path = pathlib.Path(full_path).relative_to(self.local_base).as_posix()
        is_trigger_log_context = getattr(ti, "is_trigger_log_context", False)
        self.upload_on_close = is_trigger_log_context or not ti.raw

        # Clear the file first so that duplicate data is not uploaded
        # when re-using the same path (e.g. with rescheduled sensors)
        if self.upload_on_close:
            with open(self.handler.baseFilename, "w"):  # type: ignore[union-attr]
                pass

    @staticmethod
    def encode_remote_path(remote_loc: str) -> str:
        """
        Encode URLs for compatibility with WebHDFS.

        This will replace all ":" and "+" with "-".
        """
        # We replace all URL-encode characters with hyphen because:
        # - ":" is not a valid character in HDFS paths.
        # - "+" is present in timezones from many template variables
        # - WebHDFS does not handle urlencoded correctly between versions:
        #   - https://issues.apache.org/jira/browse/HDFS-14423
        #   - https://issues.apache.org/jira/browse/HDFS-14466
        # """
        return remote_loc.replace(":", "-").replace("+", "-")

    def close(self) -> None:
        """Close and upload local log file to remote storage GCS."""
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super().close()

        if not self.upload_on_close:
            return

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.encode_remote_path(self.log_relative_path))

        if os.path.exists(local_loc):
            log = pathlib.Path(local_loc).read_text()
            self.hdfs_write(log, remote_loc)

        # Mark closed, so we don't double write if close is called twice
        self.closed = True

    def _read_remote_logs(
        self, ti: TaskInstance, try_number: int, metadata: dict[str, Any] | None = None
    ) -> tuple[list[str], list[str]]:
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        worker_log_rel_path = self._render_filename(ti, try_number)

        remote_loc = os.path.join(self.remote_base, worker_log_rel_path)
        remote_loc = self.encode_remote_path(remote_loc)

        logs = []
        messages = []
        if self.hook.check_for_path(remote_loc):
            messages.append(f"Found logs in webhdfs: {remote_loc}")
            try:
                messages.append(f"Reading remote log from HDFS: {remote_loc}.")
                logs.append(self.hook.read_file(remote_loc))
            except Exception as e:
                messages.append(f"Failed to load HDFS log file: {remote_loc}.")
                messages.append(f"{str(e)}")
        else:
            messages.append(f"No logs found on webhdfs for ti={ti}")
        return messages, logs

    def _read(
        self, ti: TaskInstance, try_number: int, metadata: dict[str, Any] | None = None
    ) -> tuple[str, dict[str, Any] | None]:
        """
        Template method that contains custom logic of reading logs given the try_number.

        todo: when min airflow version >= 2.6 then remove this method (``_read``)

        :param ti: task instance record
        :param try_number: current try_number to read log from
        :param metadata: log metadata, can be used for steaming log reading and auto-tailing.
        :return: log message as a string and metadata.
        """
        # from airflow 2.6 we no longer implement the _read method
        if hasattr(super(), "_read_remote_logs"):
            return super()._read(ti, try_number, metadata)

        # if we get here, we're on airflow < 2.6 and we use this backcompat logic
        messages, logs = self._read_remote_logs(ti, try_number, metadata)
        if logs:
            return "".join(f"*** {x}\n" for x in messages) + "\n".join(logs), {"end_of_log": True}
        else:
            if metadata and metadata.get("log_pos", 0) > 0:
                log_prefix = ""
            else:
                log_prefix = "*** Falling back to local log\n"
            local_log, metadata = super()._read(ti, try_number, metadata)
            return f"{log_prefix}{local_log}", metadata

    def hdfs_write(self, log: str, hdfs_path: str, max_retry: int = 1) -> None:
        directory = os.path.dirname(hdfs_path)

        # Default to a single retry attempt because s3 upload failures are
        # rare but occasionally occur.  Multiple retry attempts are unlikely
        # to help as they usually indicate non-empheral errors.
        for try_num in range(1 + max_retry):
            try:
                if not self.hook.check_for_path(directory):
                    self.hook.make_directory(directory)

                if self.hook.is_file(hdfs_path):
                    self.hook.append_file(log, hdfs_path)
                else:
                    self.hook.write_file(log, hdfs_path)
                break
            except Exception:
                if try_num < max_retry:
                    self.log.warning("Failed attempt to write logs to %s, will retry", hdfs_path)
                else:
                    self.log.exception("Could not write logs to %s", hdfs_path)
