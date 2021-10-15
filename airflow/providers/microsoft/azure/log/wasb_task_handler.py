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

from azure.common import AzureHttpError

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.configuration import conf
try:
    from airflow.utils.log.remote_file_task_handler import RemoteFileTaskHandler
except ImportError:
    from airflow.providers.microsoft.azure.log.remote_file_task_handler import RemoteFileTaskHandler


class WasbTaskHandler(RemoteFileTaskHandler):
    """
    WasbTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow RemoteFileTaskHandler and
    uploads to and reads from Wasb remote storage.
    """

    def __init__(
        self,
        base_log_folder: str,
        wasb_log_folder: str,
        wasb_container: str,
        filename_template: str,
    ) -> None:
        super().__init__(base_log_folder, filename_template, wasb_log_folder)
        self.wasb_container = wasb_container
        self._hook = None

    @cached_property
    def hook(self):
        """Returns WasbHook."""
        remote_conn_id = conf.get('logging', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

            return WasbHook(remote_conn_id)
        except AzureHttpError:
            self.log.exception(
                'Could not create an WasbHook with connection id "%s".'
                ' Please make sure that apache-airflow[azure] is installed'
                ' and the Wasb connection exists.',
                remote_conn_id,
            )
            return None

    def remote_write(self, log: str, remote_log_location: str, append: bool = True) -> bool:
        try:
            if append and self.remote_log_exists(remote_log_location):
                old_log = self.remote_read(remote_log_location)
                log = '\n'.join([old_log, log]) if old_log else log
        except Exception:
            self.log.exception('Could not verify previous log to append')

        try:
            self.hook.load_string(log, self.wasb_container, remote_log_location, overwrite=True)
        except AzureHttpError:
            self.log.exception('Could not write logs to %s', remote_log_location)
            return False
        else:
            return True

    def remote_read(self, remote_log_location: str) -> str:
        return self.hook.read_file(self.wasb_container, remote_log_location)

    def remote_log_exists(self, remote_log_location: str) -> bool:
        try:
            return self.hook.check_for_blob(self.wasb_container, remote_log_location)

        except Exception as e:
            self.log.debug('Exception when trying to check remote location: "%s"', e)
        return False
