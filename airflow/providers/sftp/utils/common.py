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
"""This module contains common functions for SFTP and SFTPBatch operator."""
import os

from airflow import AirflowException
from airflow.providers.ssh.hooks.ssh import SSHHook


def check_conn(obj):
    if obj.ssh_conn_id:
        if obj.ssh_hook and isinstance(obj.ssh_hook, SSHHook):
            obj.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
        else:
            obj.log.info("ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook.")
            obj.ssh_hook = SSHHook(ssh_conn_id=obj.ssh_conn_id)

    if not obj.ssh_hook:
        raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

    if obj.remote_host is not None:
        obj.log.info(
            "remote_host is provided explicitly. "
            "It will replace the remote_host which was defined "
            "in ssh_hook or predefined in connection of ssh_conn_id."
        )
        obj.ssh_hook.remote_host = obj.remote_host


def make_intermediate_dirs(sftp_client, remote_directory) -> None:
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
    except OSError:
        dirname, basename = os.path.split(remote_directory.rstrip('/'))
        make_intermediate_dirs(sftp_client, dirname)
        sftp_client.mkdir(basename)
        sftp_client.chdir(basename)
        return
