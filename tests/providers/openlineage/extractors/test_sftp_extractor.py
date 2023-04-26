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
from __future__ import annotations

import socket
from contextlib import suppress
from unittest import mock

import paramiko
import pytest
from openlineage.client.run import Dataset
from pkg_resources import parse_version

from airflow.models import DAG, Connection
from airflow.providers.openlineage.extractors.sftp import SFTPExtractor
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperation
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers_manager import ProvidersManager
from airflow.utils import timezone
from airflow.utils.module_loading import import_string

SFTP_PROVIDER_VERSION = parse_version(ProvidersManager().providers["apache-airflow-providers-sftp"].version)

with suppress(ImportError):
    SFTPOperator = import_string("airflow.providers.sftp.operators.sftp.SFTPOperator")

SCHEME = "file"

LOCAL_FILEPATH = "/path/to/local"
LOCAL_HOST = socket.gethostbyname(socket.gethostname())
LOCAL_AUTHORITY = f"{LOCAL_HOST}:{paramiko.config.SSH_PORT}"
LOCAL_DATASET = [Dataset(namespace=f"file://{LOCAL_AUTHORITY}", name=LOCAL_FILEPATH, facets=mock.ANY)]

REMOTE_FILEPATH = "/path/to/remote"
REMOTE_HOST = "remotehost"
REMOTE_PORT = 22
REMOTE_AUTHORITY = f"{REMOTE_HOST}:{REMOTE_PORT}"
REMOTE_DATASET = [Dataset(namespace=f"file://{REMOTE_AUTHORITY}", name=REMOTE_FILEPATH, facets=mock.ANY)]

CONN_ID = "sftp_default"
CONN = Connection(
    conn_id=CONN_ID,
    conn_type="sftp",
    host=REMOTE_HOST,
    port=REMOTE_PORT,
)


@pytest.mark.parametrize(
    "operation, expected",
    [
        (SFTPOperation.GET, (REMOTE_DATASET, LOCAL_DATASET)),
        (SFTPOperation.PUT, (LOCAL_DATASET, REMOTE_DATASET)),
    ],
)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn", spec=paramiko.SSHClient)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_connection", spec=Connection)
def test_extract_ssh_conn_id(get_connection, get_conn, operation, expected):
    get_connection.return_value = CONN

    dag_id = "sftp_dag"
    task_id = "sftp_task"

    task = SFTPOperator(
        task_id=task_id,
        ssh_conn_id=CONN_ID,
        dag=DAG(dag_id),
        start_date=timezone.utcnow(),
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=operation,
    )
    task_metadata = SFTPExtractor(task).extract()

    assert task_metadata.inputs == expected[0]
    assert task_metadata.outputs == expected[1]


@pytest.mark.skipif(
    SFTP_PROVIDER_VERSION < parse_version("4.0.0"),
    reason="SFTPOperator doesn't support sftp_hook as a constructor parameter in this version.",
)
@pytest.mark.parametrize(
    "operation, expected",
    [
        (SFTPOperation.GET, (REMOTE_DATASET, LOCAL_DATASET)),
        (SFTPOperation.PUT, (LOCAL_DATASET, REMOTE_DATASET)),
    ],
)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn", spec=paramiko.SSHClient)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_connection", spec=Connection)
def test_extract_sftp_hook(get_connection, get_conn, operation, expected):
    get_connection.return_value = CONN

    dag_id = "sftp_dag"
    task_id = "sftp_task"

    task = SFTPOperator(
        task_id=task_id,
        sftp_hook=SFTPHook(ssh_conn_id=CONN_ID),
        dag=DAG(dag_id),
        start_date=timezone.utcnow(),
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=operation,
    )
    task_metadata = SFTPExtractor(task).extract()

    assert task_metadata.inputs == expected[0]
    assert task_metadata.outputs == expected[1]


@pytest.mark.parametrize(
    "operation, expected",
    [
        (SFTPOperation.GET, (REMOTE_DATASET, LOCAL_DATASET)),
        (SFTPOperation.PUT, (LOCAL_DATASET, REMOTE_DATASET)),
    ],
)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn", spec=paramiko.SSHClient)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_connection", spec=Connection)
def test_extract_ssh_hook(get_connection, get_conn, operation, expected):
    get_connection.return_value = CONN

    dag_id = "sftp_dag"
    task_id = "sftp_task"

    task = SFTPOperator(
        task_id=task_id,
        ssh_hook=SSHHook(ssh_conn_id=CONN_ID),
        dag=DAG(dag_id),
        start_date=timezone.utcnow(),
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=operation,
    )
    task_metadata = SFTPExtractor(task).extract()

    assert task_metadata.inputs == expected[0]
    assert task_metadata.outputs == expected[1]
