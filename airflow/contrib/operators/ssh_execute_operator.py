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
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.ssh_hook import SshHook


class SshExecuteOperator(BaseOperator):
    """
    Execute a command on a remote host.

    :param ssh_conn_id: reference to a predefined connection
    :type ssh_conn_id: string
    :param cmd: The command to be executed
    :type cmd: string
    """

    @apply_defaults
    def __init__(self, cmd, ssh_conn_id='ssh_default', *args, **kwargs):
        super(SshExecuteOperator, self).__init__(*args, **kwargs)
        self.cmd = cmd
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context):
        logging.info('Executing: {0}'.format(self.cmd))
        self.hook = SshHook(ssh_conn_id=self.ssh_conn_id)
        self.hook.exec_command(self.cmd)
