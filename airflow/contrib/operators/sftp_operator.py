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

import logging

from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SFTPOperation(object):
    PUT = 'put'
    GET = 'get'


class SFTPOperator(BaseOperator):
    """
    SFTPOperator for transferring files from remote host to local or vice a versa.
    This operator uses ssh_hook to open sftp trasport channel that serve as basis
    for file transfer.

    :param ssh_hook: predefined ssh_hook to use for remote execution
    :type ssh_hook: :class:`SSHHook`
    :param ssh_conn_id: connection id from airflow Connections
    :type ssh_conn_id: str
    :param remote_host: remote host to connect
    :type remote_host: str
    :param local_filepath: local file path to get or put
    :type local_filepath: str
    :param remote_filepath: remote file path to get or put
    :type remote_filepath: str
    :param operation: specify operation 'get' or 'put', defaults to get
    :type get: bool
    """
    template_fields = ('local_filepath', 'remote_filepath')

    @apply_defaults
    def __init__(self,
                 ssh_hook=None,
                 ssh_conn_id=None,
                 remote_host=None,
                 local_filepath=None,
                 remote_filepath=None,
                 operation=SFTPOperation.PUT,
                 *args,
                 **kwargs):
        super(SFTPOperator, self).__init__(*args, **kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath
        self.operation = operation
        if not (self.operation.lower() == SFTPOperation.GET or self.operation.lower() == SFTPOperation.PUT):
            raise TypeError("unsupported operation value {0}, expected {1} or {2}"
                            .format(self.operation, SFTPOperation.GET, SFTPOperation.PUT))

    def execute(self, context):
        file_msg = None
        try:
            if self.ssh_conn_id and not self.ssh_hook:
                self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException("can not operate without ssh_hook or ssh_conn_id")

            if self.remote_host is not None:
                self.ssh_hook.remote_host = self.remote_host

            ssh_client = self.ssh_hook.get_conn()
            sftp_client = ssh_client.open_sftp()
            if self.operation.lower() == SFTPOperation.GET:
                file_msg = "from {0} to {1}".format(self.remote_filepath,
                                                    self.local_filepath)
                logging.debug("Starting to transfer {0}".format(file_msg))
                sftp_client.get(self.remote_filepath, self.local_filepath)
            else:
                file_msg = "from {0} to {1}".format(self.local_filepath,
                                                    self.remote_filepath)
                logging.debug("Starting to transfer file {0}".format(file_msg))
                sftp_client.put(self.local_filepath, self.remote_filepath)

        except Exception as e:
            raise AirflowException("Error while transferring {0}, error: {1}"
                                   .format(file_msg, str(e)))

        return None
