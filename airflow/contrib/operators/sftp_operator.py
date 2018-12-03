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
    This operator uses ssh_hook to open sftp transport channel that serve as basis
    for file transfer.

    :param ssh_hook: predefined ssh_hook to use for remote execution.
        Either `ssh_hook` or `ssh_conn_id` needs to be provided.
    :type ssh_hook: :class:`SSHHook`
    :param ssh_conn_id: connection id from airflow Connections.
        `ssh_conn_id` will be ingored if `ssh_hook` is provided.
    :type ssh_conn_id: str
    :param remote_host: remote host to connect (templated)
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
    :type remote_host: str
    :param local_filepath: local file path to get or put. (templated)
    :type local_filepath: str
    :param remote_filepath: remote file path to get or put. (templated)
    :type remote_filepath: str
    :param operation: specify operation 'get' or 'put', defaults to put
    :type operation: str
    :param confirm: specify if the SFTP operation should be confirmed, defaults to True
    :type confirm: bool
    :param create_intermediate_dirs: create missing intermediate directories when
        copying from remote to local and vice-versa. Default is False.

        Example: The following task would copy ``file.txt`` to the remote host
        at ``/tmp/tmp1/tmp2/`` while creating ``tmp``,``tmp1`` and ``tmp2`` if they
        don't exist. If the parameter is not passed it would error as the directory
        does not exist. ::

            put_file = SFTPOperator(
                task_id="test_sftp",
                ssh_conn="ssh_default",
                local_filepath="/tmp/file.txt",
                remote_filepath="/tmp/tmp1/tmp2/file.txt",
                operation="put",
                create_intermediate_dirs=True,
                dag=dag
            )

    :type create_intermediate_dirs: bool
    """
    template_fields = ('local_filepath', 'remote_filepath', 'remote_host')

    @apply_defaults
    def __init__(self,
                 ssh_hook=None,
                 ssh_conn_id=None,
                 remote_host=None,
                 local_filepath=None,
                 remote_filepath=None,
                 operation=SFTPOperation.PUT,
                 confirm=True,
                 create_intermediate_dirs=False,
                 *args,
                 **kwargs):
        super(SFTPOperator, self).__init__(*args, **kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath
        self.operation = operation
        self.confirm = confirm
        self.create_intermediate_dirs = create_intermediate_dirs
        if not (self.operation.lower() == SFTPOperation.GET or
                self.operation.lower() == SFTPOperation.PUT):
            raise TypeError("unsupported operation value {0}, expected {1} or {2}"
                            .format(self.operation, SFTPOperation.GET, SFTPOperation.PUT))

    def execute(self, context):
        file_msg = None
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info("ssh_hook is not provided or invalid. " +
                                  "Trying ssh_conn_id to create SSHHook.")
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                self.log.info("remote_host is provided explicitly. " +
                              "It will replace the remote_host which was defined " +
                              "in ssh_hook or predefined in connection of ssh_conn_id.")
                self.ssh_hook.remote_host = self.remote_host

            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                if self.operation.lower() == SFTPOperation.GET:
                    local_folder = os.path.dirname(self.local_filepath)
                    if self.create_intermediate_dirs:
                        # Create Intermediate Directories if it doesn't exist
                        try:
                            os.makedirs(local_folder)
                        except OSError:
                            if not os.path.isdir(local_folder):
                                raise
                    file_msg = "from {0} to {1}".format(self.remote_filepath,
                                                        self.local_filepath)
                    self.log.debug("Starting to transfer %s", file_msg)
                    sftp_client.get(self.remote_filepath, self.local_filepath)
                else:
                    remote_folder = os.path.dirname(self.remote_filepath)
                    if self.create_intermediate_dirs:
                        _make_intermediate_dirs(
                            sftp_client=sftp_client,
                            remote_directory=remote_folder,
                        )
                    file_msg = "from {0} to {1}".format(self.local_filepath,
                                                        self.remote_filepath)
                    self.log.debug("Starting to transfer file %s", file_msg)
                    sftp_client.put(self.local_filepath,
                                    self.remote_filepath,
                                    confirm=self.confirm)

        except Exception as e:
            raise AirflowException("Error while transferring {0}, error: {1}"
                                   .format(file_msg, str(e)))

        return None


def _make_intermediate_dirs(sftp_client, remote_directory):
    """
    Create all the intermediate directories in a remote host

    :param sftp_client: A Paramiko SFTP client.
    :param remote_directory: Absolute Path of the directory containing the file
    :return:
    """
    if remote_directory == '/':
        sftp_client.chdir('/')
        return
    if remote_directory == '':
        return
    try:
        sftp_client.chdir(remote_directory)
    except IOError:
        dirname, basename = os.path.split(remote_directory.rstrip('/'))
        _make_intermediate_dirs(sftp_client, dirname)
        sftp_client.mkdir(basename)
        sftp_client.chdir(basename)
        return
