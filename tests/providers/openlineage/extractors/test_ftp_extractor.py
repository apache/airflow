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

import pytest
from openlineage.client.run import Dataset

from airflow.models import DAG, Connection
from airflow.providers.openlineage.extractors.ftp import FTPExtractor
from airflow.utils import timezone
from airflow.utils.module_loading import import_string

with suppress(ImportError):
    FTPOperator = import_string("airflow.providers.ftp.operators.ftp.FTPFileTransmitOperator")
with suppress(ImportError):
    FTPOperation = import_string("airflow.providers.ftp.operators.ftp.FTPOperation")

SCHEME = "file"

LOCAL_FILEPATH = "/path/to/local"
LOCAL_HOST = socket.gethostbyname(socket.gethostname())
LOCAL_PORT = 21
LOCAL_AUTHORITY = f"{LOCAL_HOST}:{LOCAL_PORT}"
LOCAL_DATASET = [Dataset(namespace=f"{SCHEME}://{LOCAL_AUTHORITY}", name=LOCAL_FILEPATH)]

REMOTE_FILEPATH = "/path/to/remote"
REMOTE_HOST = "remotehost"
REMOTE_PORT = 21
REMOTE_AUTHORITY = f"{REMOTE_HOST}:{REMOTE_PORT}"
REMOTE_DATASET = [Dataset(namespace=f"{SCHEME}://{REMOTE_AUTHORITY}", name=REMOTE_FILEPATH)]

CONN_ID = "ftp_default"
CONN = Connection(
    conn_id=CONN_ID,
    conn_type="ftp",
    host=REMOTE_HOST,
    port=REMOTE_PORT,
)


@pytest.mark.skipif(
    FTPOperator is None,
    reason="FTPFileTransmitOperator is only available since apache-airflow-providers-ftp 3.3.0+.",
)
@mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_conn", spec=Connection)
def test_extract_get(get_conn):
    get_conn.return_value = CONN

    dag_id = "ftp_dag"
    task_id = "ftp_task"

    task = FTPOperator(
        task_id=task_id,
        ftp_conn_id=CONN_ID,
        dag=DAG(dag_id),
        start_date=timezone.utcnow(),
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=FTPOperation.GET,
    )
    task_metadata = FTPExtractor(task).extract()

    assert task_metadata.inputs == REMOTE_DATASET
    assert task_metadata.outputs == LOCAL_DATASET


@pytest.mark.skipif(
    FTPOperator is None,
    reason="FTPFileTransmitOperator is only available since apache-airflow-providers-ftp 3.3.0+.",
)
@mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_conn", spec=Connection)
def test_extract_put(get_conn):
    get_conn.return_value = CONN

    dag_id = "ftp_dag"
    task_id = "ftp_task"

    task = FTPOperator(
        task_id=task_id,
        ftp_conn_id=CONN_ID,
        dag=DAG(dag_id),
        start_date=timezone.utcnow(),
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=FTPOperation.PUT,
    )
    task_metadata = FTPExtractor(task).extract()

    assert task_metadata.inputs == LOCAL_DATASET
    assert task_metadata.outputs == REMOTE_DATASET
