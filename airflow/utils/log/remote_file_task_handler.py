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
import shutil

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.file_task_handler import FileTaskHandler


class RemoteFileTaskHandler(FileTaskHandler, LoggingMixin):
    """
    RemoteFileTaskHandler is a python log handler that handles and reads
    task instance logs. It provides the same functionality as the
    FileTaskHandler, but copies the log file to a second log folder on close.
    This can be usefull for archiving purposes, or when dealing with
    mounted cloud storage which is paid per action.
    """
    def __init__(self, base_log_folder, secondary_log_folder, filename_template):
        super(RemoteFileTaskHandler, self).__init__(base_log_folder, filename_template)
        self.secondary_log_folder = secondary_log_folder
        self.closed = False

    def close(self):
        """
        Close and copy the local log file to the secondary location
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super(RemoteFileTaskHandler, self).close()

        primary_loc = os.path.join(self.local_base, self.log_relative_path)
        secondary_loc = os.path.join(self.secondary_log_folder, self.log_relative_path)
        if os.path.exists(primary_loc):
            shutil.copyfile(primary_loc, secondary_loc)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _init_file(self):
        self._init_file_path(os.path.join(self.secondary_log_folder,
                                          self.log_relative_path))

        return super(RemoteFileTaskHandler, self)._init_file()

    def _read(self, ti, try_number):
        """
        Read logs of given task instance and try_number from the secondary location.
        If that does not exist, fallback to the default FileTaskHandler.
        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number + 1)
        secondary_loc = os.path.join(self.secondary_log_folder, log_relative_path)

        if os.path.exists(secondary_loc):
            try:
                with open(secondary_loc) as f:
                    log = "*** Reading secondary log.\n" + "".join(f.readlines())
            except Exception as e:
                log = "*** Failed to load secondary log file: {}. {}\n" \
                    .format(secondary_loc, str(e))
        else:
            log = super(RemoteFileTaskHandler, self)._read(ti, try_number)

        return log
