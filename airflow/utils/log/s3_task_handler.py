# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.file_task_handler import FileTaskHandler


class S3TaskHandler(FileTaskHandler, LoggingMixin):
    """
    S3TaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from S3 remote storage.
    """
    def __init__(self, base_log_folder, s3_log_folder, filename_template):
        super(S3TaskHandler, self).__init__(base_log_folder, filename_template)
        self.remote_base = s3_log_folder
        self.log_relative_path = ''
        self._hook = None
        self.closed = False

    def _build_hook(self):
        remote_conn_id = configuration.get('core', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.hooks.S3_hook import S3Hook
            return S3Hook(remote_conn_id)
        except:
            self.log.error(
                'Could not create an S3Hook with connection id "%s". '
                'Please make sure that airflow[s3] is installed and '
                'the S3 connection exists.', remote_conn_id
            )

    @property
    def hook(self):
        if self._hook is None:
            self._hook = self._build_hook()
        return self._hook

    def set_context(self, ti):
        super(S3TaskHandler, self).set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to S3 remote storage.
        self.log_relative_path = self._render_filename(ti, ti.try_number + 1)

    def close(self):
        """
        Close and upload local log file to remote storage S3.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super(S3TaskHandler, self).close()

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc, 'r') as logfile:
                log = logfile.read()
            self.s3_write(log, remote_loc)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number):
        """
        Read logs of given task instance and try_number from S3 remote storage.
        If failed, read the log from task instance host machine.
        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number + 1)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        if self.s3_log_exists(remote_loc):
            # If S3 remote file exists, we do not fetch logs from task instance
            # local machine even if there are errors reading remote logs, as
            # returned remote_log will contain error messages.
            remote_log = self.s3_read(remote_loc, return_error=True)
            log = '*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log)
        else:
            log = super(S3TaskHandler, self)._read(ti, try_number)

        return log

    def s3_log_exists(self, remote_log_location):
        """
        Check if remote_log_location exists in remote storage
        :param remote_log_location: log's location in remote storage
        :return: True if location exists else False
        """
        try:
            return self.hook.get_key(remote_log_location) is not None
        except Exception:
            pass
        return False

    def s3_read(self, remote_log_location, return_error=False):
        """
        Returns the log found at the remote_log_location. Returns '' if no
        logs are found or there is an error.
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        """
        try:
            return self.hook.read_key(remote_log_location)
        except:
            msg = 'Could not read logs from {}'.format(remote_log_location)
            self.log.exception(msg)
            # return error if needed
            if return_error:
                return msg

    def s3_write(self, log, remote_log_location, append=True):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.
        :param log: the log to write to the remote_log_location
        :type log: string
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool
        """
        if append and self.s3_log_exists(remote_log_location):
            old_log = self.s3_read(remote_log_location)
            log = '\n'.join([old_log, log]) if old_log else log

        try:
            self.hook.load_string(
                log,
                key=remote_log_location,
                replace=True,
                encrypt=configuration.getboolean('core', 'ENCRYPT_S3_LOGS'),
            )
        except:
            self.log.exception('Could not write logs to %s', remote_log_location)
