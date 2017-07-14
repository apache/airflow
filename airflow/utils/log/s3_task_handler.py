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
import warnings

from airflow import configuration as conf
from airflow.utils import logging as logging_utils
from airflow.utils.log.file_task_handler import FileTaskHandler


class S3TaskHandler(FileTaskHandler):
    """
    S3TaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from S3 remote storage.
    """

    def __init__(self, base_log_folder, remote_base_log_folder):
        super(S3TaskHandler, self).__init__(base_log_folder)
        self.remote_base = remote_base_log_folder
        # deprecated as of March 2016
        if not self.remote_base and conf.get('core', 'S3_LOG_FOLDER'):
            warnings.warn(
                'The S3_LOG_FOLDER conf key has been replaced by '
                'REMOTE_BASE_LOG_FOLDER. Your conf still works but please '
                'update airflow.cfg to ensure future compatibility.',
                DeprecationWarning)
            self.remote_base = conf.get('core', 'S3_LOG_FOLDER')
        self.closed = False

    def set_context(self, task_instance):
        super(S3TaskHandler, self).set_context(task_instance)
        # Local location and remote location is needed to open and
        # upload local log file to S3 remote storage.
        self.local_loc = self.get_local_loc(task_instance)
        self.remote_loc = os.path.join(self.remote_base,
                                       self.get_log_relative_path(task_instance))

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

        if os.path.exists(self.local_loc):
            # read log and remove old logs to get just the latest additions
            with open(self.local_loc, 'r') as logfile:
                log = logfile.read()
            logging_utils.S3Log().write(log, self.remote_loc)

        self.closed = True

    def read(self, task_instance):
        """
        Read logs of given task instance from S3 remote storage. If failed,
        read the log from local machine.
        :param task_instance: task instance object
        """
        log = ""
        remote_loc = os.path.join(self.remote_base, self.get_log_relative_path(task_instance))
        # TODO: check if the remote_loc exist first instead of checking
        # if remote_log here. This logic is going to modified once logs are split
        # by try_number
        remote_log = logging_utils.S3Log().read(remote_loc, return_error=True)
        if remote_log:
            log += ('*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log))
        else:
            log = super(S3TaskHandler, self).read(task_instance)

        return log
