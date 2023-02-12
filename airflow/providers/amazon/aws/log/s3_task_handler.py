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

from airflow.compat.functools import cached_property
from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class S3TaskHandler(FileTaskHandler, LoggingMixin):
    """
    S3TaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from S3 remote storage.
    """

    trigger_should_wrap = True

    def __init__(self, base_log_folder: str, s3_log_folder: str, filename_template: str | None = None):
        super().__init__(base_log_folder, filename_template)
        self.remote_base = s3_log_folder
        self.log_relative_path = ""
        self._hook = None
        self.closed = False
        self.upload_on_close = True

    @cached_property
    def hook(self):
        """Returns S3Hook."""
        return S3Hook(
            aws_conn_id=conf.get("logging", "REMOTE_LOG_CONN_ID"), transfer_config_args={"use_threads": False}
        )

    def set_context(self, ti):
        super().set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to S3 remote storage.
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
        """Close and upload local log file to remote storage S3."""
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
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            log = pathlib.Path(local_loc).read_text()
            self.s3_write(log, remote_loc)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read_remote_logs(self, ti, try_number, metadata=None):
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        worker_log_rel_path = self._render_filename(ti, try_number)

        logs = []
        messages = []
        bucket, prefix = self.hook.parse_s3_url(s3url=os.path.join(self.remote_base, worker_log_rel_path))
        keys = self.hook.list_keys(bucket_name=bucket, prefix=prefix)
        if keys:
            keys = [f"s3://{bucket}/{key}" for key in keys]
            messages.extend(["Found logs in s3:", *[f"  * {x}" for x in sorted(keys)]])
            for key in sorted(keys):
                logs.append(self.s3_read(key, return_error=True))
        else:
            messages.append(f"No logs found on s3 for ti={ti}")
        return messages, logs

    def _read(self, ti, try_number, metadata=None):
        """
        Read logs of given task instance and try_number from S3 remote storage.
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
            local_log, metadata = super()._read(ti, try_number, metadata)
            return "*** Falling back to local log\n" + local_log, metadata

    def s3_log_exists(self, remote_log_location: str) -> bool:
        """
        Check if remote_log_location exists in remote storage

        :param remote_log_location: log's location in remote storage
        :return: True if location exists else False
        """
        return self.hook.check_for_key(remote_log_location)

    def s3_read(self, remote_log_location: str, return_error: bool = False) -> str:
        """
        Returns the log found at the remote_log_location. Returns '' if no
        logs are found or there is an error.

        :param remote_log_location: the log's location in remote storage
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :return: the log found at the remote_log_location
        """
        try:
            return self.hook.read_key(remote_log_location)
        except Exception as error:
            msg = f"Could not read logs from {remote_log_location} with error: {error}"
            self.log.exception(msg)
            # return error if needed
            if return_error:
                return msg
        return ""

    def s3_write(self, log: str, remote_log_location: str, append: bool = True, max_retry: int = 1):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the remote_log_location
        :param remote_log_location: the log's location in remote storage
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :param max_retry: Maximum number of times to retry on upload failure
        """
        try:
            if append and self.s3_log_exists(remote_log_location):
                old_log = self.s3_read(remote_log_location)
                log = "\n".join([old_log, log]) if old_log else log
        except Exception:
            self.log.exception("Could not verify previous log to append")

        # Default to a single retry attempt because s3 upload failures are
        # rare but occasionally occur.  Multiple retry attempts are unlikely
        # to help as they usually indicate non-ephemeral errors.
        for try_num in range(1 + max_retry):
            try:
                self.hook.load_string(
                    log,
                    key=remote_log_location,
                    replace=True,
                    encrypt=conf.getboolean("logging", "ENCRYPT_S3_LOGS"),
                )
                break
            except Exception:
                if try_num < max_retry:
                    self.log.warning("Failed attempt to write logs to %s, will retry", remote_log_location)
                else:
                    self.log.exception("Could not write logs to %s", remote_log_location)
