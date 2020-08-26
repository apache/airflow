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

from azure.common import AzureHttpError
from cached_property import cached_property

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class LawsTaskHandler(FileTaskHandler, LoggingMixin):
    """
    LawsTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to LAWS.
    """

    def __init__(self, account_id, access_key, table_name, base_log_folder, filename_template):
        super().__init__(base_log_folder, filename_template)
        self.log_relative_path = ''
        self._ti = None
        self._hook = None
        self.closed = False
        self.upload_on_close = True
        self.account_id = account_id
        self.access_key = access_key
        self.table_name = table_name

    @cached_property
    def hook(self):
        """
        Returns LawsHook.
        """
        remote_conn_id = conf.get('logging', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.providers.microsoft.azure.hooks.laws import LawsHook

            return LawsHook(remote_conn_id, self.account_id, self.access_key, self.table_name)
        except AzureHttpError:
            self.log.error(
                'Could not create an LawsHook with connection id "%s". '
                'Please make sure that airflow[azure] is installed and '
                'the Laws connection exists.',
                remote_conn_id,
            )

    def set_context(self, ti):
        super().set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to Laws.
        self._ti = ti
        self.log_relative_path = self._render_filename(ti, ti.try_number)
        self.upload_on_close = not ti.raw

    def close(self):
        """
        Close and upload local log file to LAWS.
        """
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
        if os.path.exists(local_loc):
            with open(local_loc, 'r') as logfile:
                log = logfile.read()
            self.laws_write(log, self._ti)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number, metadata=None):
        """
        Reads local logs using super method.
        """
        return super()._read(ti, try_number)

    def laws_write(self, log, ti):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the LAWS
        :type log: str
        """
        result = self.hook.post_log(log, ti)
        return result
