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

import os
import shutil

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class RemoteFileTaskHandler(FileTaskHandler, LoggingMixin):
    """
    RemoteFileTaskHandler is an abstract python log handler that handles and reads remote task instance logs.
    Extending classes should implement the write and read functions for the respective remote backends.
    """

    def __init__(self, base_log_folder: str, filename_template: str, remote_base: str):
        super().__init__(base_log_folder, filename_template)
        self.remote_base = remote_base
        self.log_relative_path = ''
        self.upload_on_close = True
        self.closed = False

    def remote_write(self, log: str, remote_log_location: str, append: bool = True) -> bool:
        """
        Writes the log to the remote_log_location.

        :param log: the log to write to the remote_log_location
        :type log: str
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool
        :return: True if log was successfully uploaded, False otherwise
        """
        raise NotImplementedError

    def remote_read(self, remote_log_location: str) -> str:
        """
        Returns the log found at the remote_log_location. Returns '' if no
        logs are found or there is an error.

        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :return: the log found at the remote_log_location
        """
        raise NotImplementedError

    def remote_log_exists(self, remote_log_location: str) -> bool:
        """
        Check if remote_log_location exists in remote storage

        :param remote_log_location: log's location in remote storage
        :type remote_log_location: str
        :return: True if location exists else False
        """
        raise NotImplementedError

    def set_context(self, ti):
        super().set_context(ti)
        # Log relative path is used to construct local and remote
        # log path to write and read from the remote location.
        self.log_relative_path = self._render_filename(ti, ti.try_number)
        self.upload_on_close = not ti.raw

        # Clear the file first so that duplicate data is not uploaded
        # when re-using the same path (e.g. with rescheduled sensors)
        if self.upload_on_close:
            with open(self.handler.baseFilename, 'w'):
                pass

    def close(self):
        """Close and upload local log file to remote storage."""
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
            success = self.remote_write(log, remote_loc)
            keep_local = conf.getboolean('logging', 'KEEP_LOCAL_LOGS')
            if success and not keep_local:
                shutil.rmtree(os.path.dirname(local_loc))

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number, metadata=None):
        """
        Read logs of given task instance and try_number from remote storage.
        If failed, read the log from task instance host machine.

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata, can be used for steaming log reading and auto-tailing.
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        log_exists = False
        log = ""

        try:
            log_exists = self.remote_log_exists(remote_loc)
        except Exception as error:
            self.log.exception("Failed to verify remote log exists %s.", remote_loc)
            log = f'*** Failed to verify remote log exists {remote_loc}.\n{error}\n'

        if log_exists:
            try:
                remote_log = self.remote_read(remote_loc)
                log = f'*** Reading remote log from {remote_loc}.\n{remote_log}\n'
                return log, {'end_of_log': True}
            except Exception as error:
                log = f'*** Unable to read remote log from {remote_loc}\n*** {str(error)}\n\n'
                self.log.error(log)
                local_log, metadata = super()._read(ti, try_number)
                return log + local_log, metadata
        else:
            log += '*** Falling back to local log\n'
            local_log, metadata = super()._read(ti, try_number)
            return log + local_log, metadata
