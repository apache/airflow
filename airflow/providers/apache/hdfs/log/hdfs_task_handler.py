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

import os
import pathlib
import shutil
from functools import cached_property
from urllib.parse import urlsplit

from airflow.configuration import conf
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class HdfsTaskHandler(FileTaskHandler, LoggingMixin):
    """Logging handler to upload and read from HDFS."""

    def __init__(
        self, base_log_folder: str, hdfs_log_folder: str, filename_template: str | None = None, **kwargs
    ):
        super().__init__(base_log_folder, filename_template)
        self.remote_base = urlsplit(hdfs_log_folder).path
        self.log_relative_path = ""
        self._hook = None
        self.closed = False
        self.upload_on_close = True
        self.delete_local_copy = (
            kwargs["delete_local_copy"]
            if "delete_local_copy" in kwargs
            else conf.getboolean("logging", "delete_local_logs", fallback=False)
        )

    @cached_property
    def hook(self):
        """Returns WebHDFSHook."""
        return WebHDFSHook(webhdfs_conn_id=conf.get("logging", "REMOTE_LOG_CONN_ID"))

    def set_context(self, ti):
        super().set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to HDFS storage.
        full_path = self.handler.baseFilename
        self.log_relative_path = pathlib.Path(full_path).relative_to(self.local_base).as_posix()
        is_trigger_log_context = getattr(ti, "is_trigger_log_context", False)
        self.upload_on_close = is_trigger_log_context or not ti.raw
        # Clear the file first so that duplicate data is not uploaded
        # when re-using the same path (e.g. with rescheduled sensors)
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

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc) and os.path.isfile(local_loc):
            self.hook.load_file(local_loc, remote_loc)
            if self.delete_local_copy:
                shutil.rmtree(os.path.dirname(local_loc))

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read_remote_logs(self, ti, try_number, metadata=None):
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different from task instance passed
        # in set_context method.
        worker_log_rel_path = self._render_filename(ti, try_number)

        logs = []
        messages = []
        file_path = os.path.join(self.remote_base, worker_log_rel_path)
        if self.hook.check_for_path(file_path):
            logs.append(self.hook.read_file(file_path).decode("utf-8"))
        else:
            messages.append(f"No logs found on hdfs for ti={ti}")

        return messages, logs

    def _read(self, ti, try_number, metadata=None):
        """Read logs of given task instance and try_number from HDFS.
        If failed, read the log from task instance host machine.

        todo: when min airflow version >= 2.6 then remove this method (``_read``)

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
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
