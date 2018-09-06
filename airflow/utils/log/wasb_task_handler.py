# -*- coding: utf-8 -*-
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
import os
import shutil

from airflow import configuration
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.file_task_handler import FileTaskHandler
from azure.common import AzureHttpError


class WasbTaskHandler(FileTaskHandler, LoggingMixin):
    """
    WasbTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from Wasb remote storage.
    """

    def __init__(self, base_log_folder, wasb_log_folder, wasb_container,
                 filename_template, delete_local_copy):
        super(WasbTaskHandler, self).__init__(base_log_folder, filename_template)
        self.wasb_container = wasb_container
        self.remote_base = wasb_log_folder
        self.log_relative_path = ''
        self._hook = None
        self.closed = False
        self.upload_on_close = True
        self.delete_local_copy = delete_local_copy

    def _build_hook(self):
        remote_conn_id = configuration.get('core', 'REMOTE_LOG_CONN_ID')
        try:
            return WasbHook(remote_conn_id)
        except AzureHttpError:
            self.log.error(
                'Could not create an WasbHook with connection id "%s". '
                'Please make sure that airflow[azure] is installed and '
                'the Wasb connection exists.', remote_conn_id
            )

    @property
    def hook(self):
        if self._hook is None:
            self._hook = self._build_hook()
        return self._hook

    def set_context(self, ti):
        super(WasbTaskHandler, self).set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to Wasb remote storage.
        self.log_relative_path = self._render_filename(ti, ti.try_number)
        self.upload_on_close = not ti.raw

    def close(self):
        """
        Close and upload local log file to remote storage Wasb.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super(WasbTaskHandler, self).close()

        if not self.upload_on_close:
            return

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc, 'r') as logfile:
                log = logfile.read()
            self.wasb_write(log, remote_loc, append=True)

            if self.delete_local_copy:
                shutil.rmtree(os.path.dirname(local_loc))
        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number, metadata=None):
        """
        Read logs of given task instance and try_number from Wasb remote storage.
        If failed, read the log from task instance host machine.
        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        if self.wasb_log_exists(remote_loc):
            # If Wasb remote file exists, we do not fetch logs from task instance
            # local machine even if there are errors reading remote logs, as
            # returned remote_log will contain error messages.
            remote_log = self.wasb_read(remote_loc, return_error=True)
            log = '*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log)
            return log, {'end_of_log': True}
        else:
            return super(WasbTaskHandler, self)._read(ti, try_number)

    def wasb_log_exists(self, remote_log_location):
        """
        Check if remote_log_location exists in remote storage
        :param remote_log_location: log's location in remote storage
        :return: True if location exists else False
        """
        try:
            return self.hook.check_for_blob(self.wasb_container, remote_log_location)
        except Exception:
            pass
        return False

    def wasb_read(self, remote_log_location, return_error=False):
        """
        Returns the log found at the remote_log_location. Returns '' if no
        logs are found or there is an error.
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        """
        try:
            return self.hook.read_file(self.wasb_container, remote_log_location)
        except AzureHttpError:
            msg = 'Could not read logs from {}'.format(remote_log_location)
            self.log.exception(msg)
            # return error if needed
            if return_error:
                return msg

    def wasb_write(self, log, remote_log_location, append=True):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.
        :param log: the log to write to the remote_log_location
        :type log: str
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool
        """
        if append and self.wasb_log_exists(remote_log_location):
            old_log = self.wasb_read(remote_log_location)
            log = '\n'.join([old_log, log]) if old_log else log

        try:
            self.hook.load_string(
                log,
                self.wasb_container,
                remote_log_location,
            )
        except AzureHttpError:
            self.log.exception('Could not write logs to %s',
                               remote_log_location)
