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

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.configuration import conf
from airflow.utils.log.remote_file_task_handler import RemoteFileTaskHandler


class S3TaskHandler(RemoteFileTaskHandler):
    """
    S3TaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from S3 remote storage.
    """

    def __init__(self, base_log_folder: str, s3_log_folder: str, filename_template: str):
        super().__init__(base_log_folder, filename_template, s3_log_folder)
        self._hook = None

    @cached_property
    def hook(self):
        """Returns S3Hook."""
        remote_conn_id = conf.get('logging', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            return S3Hook(remote_conn_id, transfer_config_args={"use_threads": False})
        except Exception as e:
            self.log.exception(
                'Could not create an S3Hook with connection id "%s". '
                'Please make sure that apache-airflow[aws] is installed and '
                'the S3 connection exists. Exception : "%s"',
                remote_conn_id,
                e,
            )
            return None

    def remote_write(self, log: str, remote_log_location: str, append: bool = True) -> bool:
        try:
            if append and self.remote_log_exists(remote_log_location):
                old_log = self.s3_read(remote_log_location)
                log = '\n'.join([old_log, log]) if old_log else log
        except Exception:
            self.log.exception('Could not verify previous log to append')

        try:
            self.hook.load_string(
                log,
                key=remote_log_location,
                replace=True,
                encrypt=conf.getboolean('logging', 'ENCRYPT_S3_LOGS'),
            )
        except Exception:
            self.log.exception('Could not write logs to %s', remote_log_location)
            return False
        else:
            return True

    def remote_read(self, location: str) -> str:
        return self.s3_read(location, True)

    def remote_log_exists(self, remote_log_location: str) -> bool:
        return self.hook.check_for_key(remote_log_location)

    def s3_read(self, remote_log_location: str, return_error: bool = False) -> str:
        try:
            return self.hook.read_key(remote_log_location)
        except Exception as error:
            msg = f'Could not read logs from {remote_log_location} with error: {error}'
            self.log.exception(msg)
            # return error if needed
            if return_error:
                return msg
        return ''
