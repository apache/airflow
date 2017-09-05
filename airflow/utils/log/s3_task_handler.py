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

from airflow.utils import logging as logging_utils
from airflow.utils.log.file_task_handler import FileTaskHandler


class S3TaskHandler(FileTaskHandler):
    """
    S3TaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from S3 remote storage.
    """

    def __init__(self, base_log_folder, s3_log_folder, filename_template):
        super(S3TaskHandler, self).__init__(base_log_folder, filename_template)
        self.remote_base = s3_log_folder
        self.log_relative_path = ''
        self.closed = False

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
            logging_utils.S3Log().write(log, remote_loc)

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

        s3_log = logging_utils.S3Log()
        if s3_log.log_exists(remote_loc):
            # If S3 remote file exists, we do not fetch logs from task instance
            # local machine even if there are errors reading remote logs, as
            # returned remote_log will contain error messages.
            remote_log = s3_log.read(remote_loc, return_error=True)
            log = '*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log)
        else:
            log = super(S3TaskHandler, self)._read(ti, try_number)

        return log
