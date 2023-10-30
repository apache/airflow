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
import shutil
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from azure.core.exceptions import HttpResponseError
from packaging.version import Version

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    import logging


def get_default_delete_local_copy():
    """Load delete_local_logs conf if Airflow version > 2.6 and return False if not.

    TODO: delete this function when min airflow version >= 2.6
    """
    from airflow.version import version

    if Version(version) < Version("2.6"):
        return False
    return conf.getboolean("logging", "delete_local_logs")


class WasbTaskHandler(FileTaskHandler, LoggingMixin):
    """
    WasbTaskHandler is a python log handler that handles and reads task instance logs.

    It extends airflow FileTaskHandler and uploads to and reads from Wasb remote storage.
    """

    trigger_should_wrap = True

    def __init__(
        self,
        base_log_folder: str,
        wasb_log_folder: str,
        wasb_container: str,
        *,
        filename_template: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(base_log_folder, filename_template)
        self.handler: logging.FileHandler | None = None
        self.wasb_container = wasb_container
        self.remote_base = wasb_log_folder
        self.log_relative_path = ""
        self.closed = False
        self.upload_on_close = True
        self.delete_local_copy = (
            kwargs["delete_local_copy"] if "delete_local_copy" in kwargs else get_default_delete_local_copy()
        )

    @cached_property
    def hook(self):
        """Returns WasbHook."""
        remote_conn_id = conf.get("logging", "REMOTE_LOG_CONN_ID")
        try:
            from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

            return WasbHook(remote_conn_id)
        except Exception:
            self.log.exception(
                "Could not create a WasbHook with connection id '%s'. "
                "Do you have apache-airflow[azure] installed? "
                "Does connection the connection exist, and is it "
                "configured properly?",
                remote_conn_id,
            )
            return None

    def set_context(self, ti) -> None:
        super().set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to Wasb remote storage.
        if TYPE_CHECKING:
            assert self.handler is not None

        full_path = self.handler.baseFilename
        self.log_relative_path = Path(full_path).relative_to(self.local_base).as_posix()
        is_trigger_log_context = getattr(ti, "is_trigger_log_context", False)
        self.upload_on_close = is_trigger_log_context or not ti.raw

    def close(self) -> None:
        """Close and upload local log file to remote storage Wasb."""
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
            with open(local_loc) as logfile:
                log = logfile.read()
            wasb_write = self.wasb_write(log, remote_loc, append=True)

            if wasb_write and self.delete_local_copy:
                shutil.rmtree(os.path.dirname(local_loc))
        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read_remote_logs(self, ti, try_number, metadata=None) -> tuple[list[str], list[str]]:
        messages = []
        logs = []
        worker_log_relative_path = self._render_filename(ti, try_number)
        # TODO: fix this - "relative path" i.e currently REMOTE_BASE_LOG_FOLDER should start with "wasb"
        # unlike others with shceme in URL itself to identify the correct handler.
        # This puts limitations on ways users can name the base_path.
        prefix = os.path.join(self.remote_base, worker_log_relative_path)
        blob_names = []
        try:
            blob_names = self.hook.get_blobs_list(container_name=self.wasb_container, prefix=prefix)
        except HttpResponseError as e:
            messages.append(f"tried listing blobs with prefix={prefix} and container={self.wasb_container}")
            messages.append(f"could not list blobs {e}")
            self.log.exception("can't list blobs")

        if blob_names:
            uris = [f"https://{self.wasb_container}.blob.core.windows.net/{b}" for b in blob_names]
            messages.extend(["Found remote logs:", *[f"  * {x}" for x in sorted(uris)]])
        else:
            messages.append(f"No logs found in WASB; ti=%s {ti}")

        for name in sorted(blob_names):
            remote_log = ""
            try:
                remote_log = self.hook.read_file(self.wasb_container, name)
                if remote_log:
                    logs.append(remote_log)
            except Exception as e:
                messages.append(
                    f"Unable to read remote blob '{name}' in container '{self.wasb_container}'\n{e}"
                )
                self.log.exception("Could not read blob")
        return messages, logs

    def _read(
        self, ti, try_number: int, metadata: dict[str, Any] | None = None
    ) -> tuple[str, dict[str, bool]]:
        """
        Read logs of given task instance and try_number from Wasb remote storage.

        If failed, read the log from task instance host machine.

        todo: when min airflow version >= 2.6, remove this method

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        """
        if hasattr(super(), "_read_remote_logs"):
            # from Airflow 2.6, we don't implement the `_read` method.
            # if parent has _read_remote_logs, we're >= 2.6
            return super()._read(ti, try_number, metadata)

        # below is backcompat, for airflow < 2.6
        messages, logs = self._read_remote_logs(ti, try_number, metadata)
        if not logs:
            return super()._read(ti, try_number, metadata)
        return "".join([f"*** {x}\n" for x in messages]) + "\n".join(logs), {"end_of_log": True}

    def wasb_log_exists(self, remote_log_location: str) -> bool:
        """
        Check if remote_log_location exists in remote storage.

        :param remote_log_location: log's location in remote storage
        :return: True if location exists else False
        """
        try:
            return self.hook.check_for_blob(self.wasb_container, remote_log_location)

        except Exception as e:
            self.log.debug('Exception when trying to check remote location: "%s"', e)
        return False

    def wasb_read(self, remote_log_location: str, return_error: bool = False):
        """
        Return the log found at the remote_log_location. Returns '' if no logs are found or there is an error.

        :param remote_log_location: the log's location in remote storage
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        """
        try:
            return self.hook.read_file(self.wasb_container, remote_log_location)
        except Exception:
            msg = f"Could not read logs from {remote_log_location}"
            self.log.exception(msg)
            # return error if needed
            if return_error:
                return msg
            return ""

    def wasb_write(self, log: str, remote_log_location: str, append: bool = True) -> bool:
        """
        Writes the log to the remote_log_location. Fails silently if no hook was created.

        :param log: the log to write to the remote_log_location
        :param remote_log_location: the log's location in remote storage
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        """
        if append and self.wasb_log_exists(remote_log_location):
            old_log = self.wasb_read(remote_log_location)
            log = f"{old_log}\n{log}" if old_log else log

        try:
            self.hook.load_string(log, self.wasb_container, remote_log_location, overwrite=True)
        except Exception:
            self.log.exception("Could not write logs to %s", remote_log_location)
            return False
        return True
