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
from __future__ import annotations

import socket
from unittest import mock

import pytest

from airflow.models import DAG, Connection
from airflow.providers.common.compat.openlineage.facet import Dataset
from airflow.providers.ftp.operators.ftp import (
    FTPFileTransmitOperator,
    FTPOperation,
    FTPSFileTransmitOperator,
)
from airflow.utils import timezone
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2017, 1, 1)
DEFAULT_CONN_ID = "ftp_default"


class TestFTPFileTransmitOperator:
    def setup_method(self):
        self.test_local_dir = "ftptmp"
        self.test_remote_dir = "/ftphome"
        self.test_remote_dir_int = "/ftphome/interdir"
        self.test_local_filename = "test_local_file"
        self.test_remote_filename = "test_remote_file"
        self.test_local_filepath = f"{self.test_local_dir}/{self.test_local_filename}"
        self.test_remote_filepath = f"{self.test_remote_dir}/{self.test_remote_filename}"
        self.test_local_filepath_int_dir = f"{self.test_local_dir}/{self.test_local_filename}"
        self.test_remote_filepath_int_dir = f"{self.test_remote_dir_int}/{self.test_remote_filename}"

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPHook.store_file")
    @mock.patch("airflow.providers.ftp.operators.ftp.FTPHook.create_directory")
    def test_file_transfer_put(self, mock_create_dir, mock_put):
        ftp_op = FTPFileTransmitOperator(
            task_id="test_ftp_put",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.PUT,
        )
        ftp_op.execute(None)
        assert not mock_create_dir.called
        mock_put.assert_called_once_with(self.test_remote_filepath, self.test_local_filepath)

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPHook.store_file")
    @mock.patch("airflow.providers.ftp.operators.ftp.FTPHook.create_directory")
    def test_file_transfer_with_intermediate_dir_put(self, mock_create_dir, mock_put):
        ftp_op = FTPFileTransmitOperator(
            task_id="test_ftp_put_imm_dirs",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath_int_dir,
            operation=FTPOperation.PUT,
            create_intermediate_dirs=True,
        )
        ftp_op.execute(None)
        mock_create_dir.assert_called_with(self.test_remote_dir_int)
        mock_put.assert_called_once_with(self.test_remote_filepath_int_dir, self.test_local_filepath)

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPHook.retrieve_file")
    def test_file_transfer_get(self, mock_get):
        ftp_op = FTPFileTransmitOperator(
            task_id="test_ftp_get",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.GET,
        )
        ftp_op.execute(None)
        mock_get.assert_called_once_with(self.test_remote_filepath, self.test_local_filepath)

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPHook.retrieve_file")
    def test_file_transfer_with_intermediate_dir_get(self, mock_get, tmp_path):
        ftp_op = FTPFileTransmitOperator(
            task_id="test_ftp_get_imm_dirs",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=str(tmp_path / self.test_local_filepath_int_dir),
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.GET,
            create_intermediate_dirs=True,
        )
        ftp_op.execute(None)
        assert len(list(tmp_path.iterdir())) == 1
        mock_get.assert_called_once_with(
            self.test_remote_filepath, str(tmp_path / self.test_local_filepath_int_dir)
        )

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPHook.retrieve_file")
    def test_multiple_paths_get(self, mock_get):
        local_filepath = ["/tmp/ltest1", "/tmp/ltest2"]
        remote_filepath = ["/tmp/rtest1", "/tmp/rtest2"]
        ftp_op = FTPFileTransmitOperator(
            task_id="test_multiple_paths_get",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=FTPOperation.GET,
        )
        ftp_op.execute(None)
        assert mock_get.call_count == 2
        for count, (args, _) in enumerate(mock_get.call_args_list):
            assert args == (remote_filepath[count], local_filepath[count])

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPHook.store_file")
    def test_multiple_paths_put(self, mock_put):
        local_filepath = ["/tmp/ltest1", "/tmp/ltest2"]
        remote_filepath = ["/tmp/rtest1", "/tmp/rtest2"]
        ftp_op = FTPFileTransmitOperator(
            task_id="test_multiple_paths_put",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=FTPOperation.PUT,
        )
        ftp_op.execute(None)
        assert mock_put.call_count == 2
        for count, (args, _) in enumerate(mock_put.call_args_list):
            assert args == (remote_filepath[count], local_filepath[count])

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPHook.store_file")
    def test_arg_checking(self, mock_put):
        dag = DAG(
            dag_id="unit_tests_ftp_op_arg_checking",
            schedule=None,
            default_args={"start_date": DEFAULT_DATE},
        )
        # If ftp_conn_id is not passed in, it should be assigned the default connection id
        task_0 = FTPFileTransmitOperator(
            task_id="test_ftp_args_0",
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.PUT,
            dag=dag,
        )
        task_0.execute(None)
        assert task_0.ftp_conn_id == DEFAULT_CONN_ID

        # Exception should be raised if operation is invalid
        task_1 = FTPFileTransmitOperator(
            task_id="test_ftp_args_1",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation="invalid_operation",
            dag=dag,
        )
        with pytest.raises(TypeError, match="Unsupported operation value invalid_operation, "):
            task_1.execute(None)

    def test_unequal_local_remote_file_paths(self):
        with pytest.raises(ValueError, match="1 paths in local_filepath != 2 paths in remote_filepath"):
            FTPFileTransmitOperator(
                task_id="test_ftp_unequal_paths",
                ftp_conn_id=DEFAULT_CONN_ID,
                local_filepath="/tmp/test",
                remote_filepath=["/tmp/test1", "/tmp/test2"],
            ).execute(None)

        with pytest.raises(ValueError, match="2 paths in local_filepath != 1 paths in remote_filepath"):
            FTPFileTransmitOperator(
                task_id="test_ftp_unequal_paths",
                ftp_conn_id=DEFAULT_CONN_ID,
                local_filepath=["/tmp/test1", "/tmp/test2"],
                remote_filepath="/tmp/test1",
            ).execute(None)


class TestFTPSFileTransmitOperator:
    def setup_method(self):
        self.test_local_dir = "ftpstmp"
        self.test_remote_dir = "/ftpshome"
        self.test_remote_dir_int = "/ftpshome/interdir"
        self.test_local_filename = "test_local_file"
        self.test_remote_filename = "test_remote_file"
        self.test_local_filepath = f"{self.test_local_dir}/{self.test_local_filename}"
        self.test_remote_filepath = f"{self.test_remote_dir}/{self.test_remote_filename}"
        self.test_local_filepath_int_dir = f"{self.test_local_dir}/{self.test_local_filename}"
        self.test_remote_filepath_int_dir = f"{self.test_remote_dir_int}/{self.test_remote_filename}"

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPSHook.store_file")
    @mock.patch("airflow.providers.ftp.operators.ftp.FTPSHook.create_directory")
    def test_file_transfer_put(self, mock_create_dir, mock_put):
        ftps_op = FTPSFileTransmitOperator(
            task_id="test_ftps_put",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.PUT,
        )
        ftps_op.execute(None)
        assert not mock_create_dir.called
        mock_put.assert_called_once_with(self.test_remote_filepath, self.test_local_filepath)

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPSHook.store_file")
    @mock.patch("airflow.providers.ftp.operators.ftp.FTPSHook.create_directory")
    def test_file_transfer_with_intermediate_dir_put(self, mock_create_dir, mock_put):
        ftps_op = FTPSFileTransmitOperator(
            task_id="test_ftps_put_imm_dirs",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath_int_dir,
            operation=FTPOperation.PUT,
            create_intermediate_dirs=True,
        )
        ftps_op.execute(None)
        mock_create_dir.assert_called_with(self.test_remote_dir_int)
        mock_put.assert_called_once_with(self.test_remote_filepath_int_dir, self.test_local_filepath)

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPSHook.retrieve_file")
    def test_file_transfer_get(self, mock_get):
        ftps_op = FTPSFileTransmitOperator(
            task_id="test_ftps_get",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.GET,
        )
        ftps_op.execute(None)
        mock_get.assert_called_once_with(self.test_remote_filepath, self.test_local_filepath)

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPHook.retrieve_file")
    def test_file_transfer_with_intermediate_dir_get(self, mock_get, tmp_path):
        ftp_op = FTPFileTransmitOperator(
            task_id="test_ftp_get_imm_dirs",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=str(tmp_path / self.test_local_filepath_int_dir),
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.GET,
            create_intermediate_dirs=True,
        )
        ftp_op.execute(None)
        assert len(list(tmp_path.iterdir())) == 1
        mock_get.assert_called_once_with(
            self.test_remote_filepath, str(tmp_path / self.test_local_filepath_int_dir)
        )

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPSHook.retrieve_file")
    def test_multiple_paths_get(self, mock_get):
        local_filepath = ["/tmp/ltest1", "/tmp/ltest2"]
        remote_filepath = ["/tmp/rtest1", "/tmp/rtest2"]
        ftps_op = FTPSFileTransmitOperator(
            task_id="test_multiple_paths_get",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=FTPOperation.GET,
        )
        ftps_op.execute(None)
        assert mock_get.call_count == 2
        for count, (args, _) in enumerate(mock_get.call_args_list):
            assert args == (remote_filepath[count], local_filepath[count])

    @mock.patch("airflow.providers.ftp.operators.ftp.FTPSHook.store_file")
    def test_multiple_paths_put(self, mock_put):
        local_filepath = ["/tmp/ltest1", "/tmp/ltest2"]
        remote_filepath = ["/tmp/rtest1", "/tmp/rtest2"]
        ftps_op = FTPSFileTransmitOperator(
            task_id="test_multiple_paths_put",
            ftp_conn_id=DEFAULT_CONN_ID,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=FTPOperation.PUT,
        )
        ftps_op.execute(None)
        assert mock_put.call_count == 2
        for count, (args, _) in enumerate(mock_put.call_args_list):
            assert args == (remote_filepath[count], local_filepath[count])

    @mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_conn", spec=Connection)
    def test_extract_get(self, get_conn):
        get_conn.return_value = Connection(
            conn_id="ftp_conn_id",
            conn_type="ftp",
            host="remotehost",
            port=21,
        )

        dag_id = "ftp_dag"
        task_id = "ftp_task"

        task = FTPFileTransmitOperator(
            task_id=task_id,
            ftp_conn_id="ftp_conn_id",
            dag=DAG(dag_id, schedule=None),
            start_date=timezone.utcnow(),
            local_filepath="/path/to/local",
            remote_filepath="/path/to/remote",
            operation=FTPOperation.GET,
        )
        lineage = task.get_openlineage_facets_on_start()

        assert lineage.inputs == [Dataset(namespace="file://remotehost:21", name="/path/to/remote")]
        assert lineage.outputs == [
            Dataset(
                namespace=f"file://{socket.gethostbyname(socket.gethostname())}:21", name="/path/to/local"
            )
        ]

    @mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_conn", spec=Connection)
    def test_extract_put(self, get_conn):
        get_conn.return_value = Connection(
            conn_id="ftp_conn_id",
            conn_type="ftp",
            host="remotehost",
            port=21,
        )

        dag_id = "ftp_dag"
        task_id = "ftp_task"

        task = FTPFileTransmitOperator(
            task_id=task_id,
            ftp_conn_id="ftp_conn_id",
            dag=DAG(dag_id, schedule=None),
            start_date=timezone.utcnow(),
            local_filepath="/path/to/local",
            remote_filepath="/path/to/remote",
            operation=FTPOperation.PUT,
        )
        lineage = task.get_openlineage_facets_on_start()

        assert lineage.inputs == [
            Dataset(
                namespace=f"file://{socket.gethostbyname(socket.gethostname())}:21", name="/path/to/local"
            )
        ]
        assert lineage.outputs == [Dataset(namespace="file://remotehost:21", name="/path/to/remote")]
