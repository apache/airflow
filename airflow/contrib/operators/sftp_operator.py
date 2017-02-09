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

from airflow.models import BaseOperator
from airflow.contrib.hooks.ssh_hook import SshHook
from airflow.utils.decorators import apply_defaults


class SftpOperator(BaseOperator):
    """
    Transfers file via SFTP put/get methods.

    :param sftp_conn_id: reference to a remote filesystem
    :param local_filepath: Full path to file on local filesystem
    :type local_filepath: string
    :param remote_filepath: Full path to file on remote filesystem
    :type remote_filepath: string
    :param put: If put is True, will copy local_filepath to remote_filepath.
        If put is False, will copy remote_filepath to local_filepath
    :type put: bool
    """

    @apply_defaults
    def __init__(self, sftp_conn_id, local_filepath, remote_filepath, put=False, *args, **kwargs):

        super(SftpOperator, self).__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath
        self.put = put

    def execute(self, context):
        """
        Transfer file via SFTP
        """
        hook = SshHook(ssh_conn_id=self.sftp_conn_id)

        if self.put:
            logging.info('Will PUT file on remote server')
            hook.put(self.local_filepath, self.remote_filepath)
        else:
            logging.info('Will GET file from remote server')
            hook.get(self.remote_filepath, self.local_filepath)
