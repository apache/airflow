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

import contextlib
import errno
import logging
import os
import socket
from base64 import b64encode
from unittest import mock

import paramiko
import pytest

from airflow.exceptions import TaskDeferred
from airflow.models import DAG, Connection
from airflow.providers.common.compat.openlineage.facet import Dataset
from airflow.providers.common.compat.sdk import AirflowException, timezone
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperation, SFTPOperator
from airflow.providers.sftp.triggers.sftp import SFTPTransferTrigger
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2017, 1, 1)
TEST_CONN_ID = "conn_id_for_testing"

LOCAL_FILEPATH = "/path/local"
REMOTE_FILEPATH = "/path/remote"
LOCAL_DATASET = [
    Dataset(namespace=f"file://{socket.gethostbyname(socket.gethostname())}:22", name=LOCAL_FILEPATH)
]
REMOTE_DATASET = [Dataset(namespace="file://remotehost:22", name=REMOTE_FILEPATH)]

TEST_GET_PUT_PARAMS = [
    (SFTPOperation.GET, (REMOTE_DATASET, LOCAL_DATASET)),
    (SFTPOperation.PUT, (LOCAL_DATASET, REMOTE_DATASET)),
]


class TestSFTPOperator:
    def setup_method(self):
        hook = SSHHook(ssh_conn_id="ssh_default")
        hook.no_host_key_check = True
        self.hook = hook
        sftp_hook = SFTPHook(ssh_conn_id="ssh_default")
        sftp_hook.no_host_key_check = True
        self.sftp_hook = sftp_hook
        self.test_dir = "/tmp"
        self.test_local_dir = "/tmp/tmp2"
        self.test_remote_dir = "/tmp/tmp1"
        self.test_local_filename = "test_local_file"
        self.test_remote_filename = "test_remote_file"
        self.test_remote_file_content = (
            b"This is remote file content \n which is also multiline "
            b"another line here \n this is last line. EOF"
        )
        self.test_local_filepath = f"{self.test_dir}/{self.test_local_filename}"
        # Local Filepath with Intermediate Directory
        self.test_local_filepath_int_dir = f"{self.test_local_dir}/{self.test_local_filename}"
        self.test_remote_filepath = f"{self.test_dir}/{self.test_remote_filename}"
        # Remote Filepath with Intermediate Directory
        self.test_remote_filepath_int_dir = f"{self.test_remote_dir}/{self.test_remote_filename}"

    def teardown_method(self):
        if os.path.exists(self.test_local_filepath):
            os.remove(self.test_local_filepath)
        if os.path.exists(self.test_local_filepath_int_dir):
            os.remove(self.test_local_filepath_int_dir)
        if os.path.exists(self.test_local_dir):
            os.rmdir(self.test_local_dir)
        if os.path.exists(self.test_remote_filepath):
            os.remove(self.test_remote_filepath)
        if os.path.exists(self.test_remote_filepath_int_dir):
            os.remove(self.test_remote_filepath_int_dir)
        if os.path.exists(self.test_remote_dir):
            os.rmdir(self.test_remote_dir)

    def test_default_args(self):
        operator = SFTPOperator(
            task_id="test_default_args",
            remote_filepath="/tmp/remote_file",
        )
        assert operator.operation == SFTPOperation.PUT
        assert operator.confirm is True
        assert operator.create_intermediate_dirs is False
        assert operator.concurrency == 1
        assert operator.prefetch is True
        assert operator.local_filepath is None
        assert operator.sftp_hook is None
        assert operator.ssh_conn_id is None
        assert operator.remote_host is None
        assert operator.deferrable is False

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Pickle support is removed in Airflow 3")
    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_pickle_file_transfer_put(self, dag_maker):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )
        # create a test file locally
        with open(self.test_local_filepath, "wb") as file:
            file.write(test_local_file_content)

        with dag_maker(dag_id="unit_tests_sftp_op_pickle_file_transfer_put", start_date=DEFAULT_DATE):
            SFTPOperator(  # Put test file to remote.
                task_id="put_test_task",
                sftp_hook=self.sftp_hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.PUT,
                create_intermediate_dirs=True,
            )
            SSHOperator(  # Check the remote file content.
                task_id="check_file_task",
                ssh_hook=self.hook,
                command=f"cat {self.test_remote_filepath}",
                do_xcom_push=True,
            )

        dr = dag_maker.create_dagrun()
        dag_maker.run_ti("put_test_task", dr)
        check_file_task_ti = dag_maker.run_ti("check_file_task", dr)

        pulled = check_file_task_ti.xcom_pull(task_ids="check_file_task", key="return_value")
        assert pulled.strip() == test_local_file_content

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Pickle support is removed in Airflow 3")
    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_file_transfer_no_intermediate_dir_error_put(self, dag_maker):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )
        # create a test file locally
        with open(self.test_local_filepath, "wb") as file:
            file.write(test_local_file_content)

        # Try to put test file to remote. This should raise an error with
        # "No such file" as the directory does not exist.
        with dag_maker(dag_id="unit_tests_sftp_op_file_transfer_no_intermediate_dir_error_put"):
            SFTPOperator(
                task_id="test_sftp",
                sftp_hook=self.sftp_hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath_int_dir,
                operation=SFTPOperation.PUT,
                create_intermediate_dirs=False,
            )
        with pytest.raises(AirflowException) as ctx:
            dag_maker.run_ti("test_sftp")
        assert "No such file" in str(ctx.value)

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Pickle support is removed in Airflow 3")
    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_file_transfer_with_intermediate_dir_put(self, dag_maker):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )
        # create a test file locally
        with open(self.test_local_filepath, "wb") as file:
            file.write(test_local_file_content)

        with dag_maker(dag_id="unit_tests_sftp_op_file_transfer_with_intermediate_dir_put"):
            SFTPOperator(  # Put test file to remote.
                task_id="test_sftp",
                sftp_hook=self.sftp_hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath_int_dir,
                operation=SFTPOperation.PUT,
                create_intermediate_dirs=True,
            )
            SSHOperator(  # Check the remote file content.
                task_id="test_check_file",
                ssh_hook=self.hook,
                command=f"cat {self.test_remote_filepath_int_dir}",
                do_xcom_push=True,
            )
        dagrun = dag_maker.create_dagrun(logical_date=timezone.utcnow())
        dag_maker.run_ti("test_sftp", dagrun)
        test_check_file_ti = dag_maker.run_ti("test_check_file", dagrun)
        pulled = test_check_file_ti.xcom_pull(task_ids="test_check_file", key="return_value")
        assert pulled.strip() == test_local_file_content

    @conf_vars({("core", "enable_xcom_pickling"): "False"})
    def test_json_file_transfer_put(self, dag_maker):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )
        # create a test file locally
        with open(self.test_local_filepath, "wb") as file:
            file.write(test_local_file_content)

        with dag_maker(dag_id="unit_tests_sftp_op_json_file_transfer_put"):
            SFTPOperator(  # Put test file to remote.
                task_id="put_test_task",
                sftp_hook=self.sftp_hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.PUT,
            )
            SSHOperator(  # Check the remote file content.
                task_id="check_file_task",
                ssh_hook=self.hook,
                command=f"cat {self.test_remote_filepath}",
                do_xcom_push=True,
            )
        dagrun = dag_maker.create_dagrun(logical_date=timezone.utcnow())
        dag_maker.run_ti("put_test_task", dagrun)
        check_file_task_ti = dag_maker.run_ti("check_file_task", dagrun)

        pulled = check_file_task_ti.xcom_pull(task_ids="check_file_task", key="return_value")
        assert pulled.strip() == b64encode(test_local_file_content).decode("utf-8")

    @pytest.fixture
    def create_remote_file_and_cleanup(self):
        with open(self.test_remote_filepath, "wb") as file:
            file.write(self.test_remote_file_content)
        yield
        os.remove(self.test_remote_filepath)

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Pickle support is removed in Airflow 3")
    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_pickle_file_transfer_get(self, dag_maker, create_remote_file_and_cleanup):
        with dag_maker(dag_id="unit_tests_sftp_op_pickle_file_transfer_get"):
            SFTPOperator(  # Get remote file to local.
                task_id="test_sftp",
                sftp_hook=self.sftp_hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.GET,
            )
        dag_maker.run_ti("test_sftp")

        # Test the received content.
        with open(self.test_local_filepath, "rb") as file:
            content_received = file.read()
        assert content_received == self.test_remote_file_content

    @conf_vars({("core", "enable_xcom_pickling"): "False"})
    def test_json_file_transfer_get(self, dag_maker, create_remote_file_and_cleanup):
        with dag_maker(dag_id="unit_tests_sftp_op_json_file_transfer_get"):
            SFTPOperator(  # Get remote file to local.
                task_id="test_sftp",
                sftp_hook=self.sftp_hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.GET,
            )
        dag_maker.run_ti("test_sftp")

        # Test the received content.
        content_received = None
        with open(self.test_local_filepath, "rb") as file:
            content_received = file.read()
        assert content_received == self.test_remote_file_content

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Pickle support is removed in Airflow 3")
    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_file_transfer_no_intermediate_dir_error_get(self, dag_maker, create_remote_file_and_cleanup):
        with dag_maker(dag_id="unit_tests_sftp_op_file_transfer_no_intermediate_dir_error_get"):
            SFTPOperator(  # Try to GET test file from remote.
                task_id="test_sftp",
                sftp_hook=self.sftp_hook,
                local_filepath=self.test_local_filepath_int_dir,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.GET,
            )

        # This should raise an error with "No such file" as the directory
        # does not exist.
        with pytest.raises(AirflowException) as ctx:
            dag_maker.run_ti("test_sftp")
        assert "No such file" in str(ctx.value)

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Pickle support is removed in Airflow 3")
    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_file_transfer_with_intermediate_dir_error_get(self, dag_maker, create_remote_file_and_cleanup):
        with dag_maker(dag_id="unit_tests_sftp_op_file_transfer_with_intermediate_dir_error_get"):
            SFTPOperator(  # Get remote file to local.
                task_id="test_sftp",
                sftp_hook=self.sftp_hook,
                local_filepath=self.test_local_filepath_int_dir,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.GET,
                create_intermediate_dirs=True,
            )
        dag_maker.run_ti("test_sftp")

        # Test the received content.
        content_received = None
        with open(self.test_local_filepath_int_dir, "rb") as file:
            content_received = file.read()
        assert content_received == self.test_remote_file_content

    @mock.patch.dict("os.environ", {"AIRFLOW_CONN_" + TEST_CONN_ID.upper(): "ssh://test_id@localhost"})
    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.retrieve_directory")
    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.retrieve_file")
    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.create_directory")
    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.store_file")
    def test_arg_checking(self, mock_store_file, mock_create_dir, mock_retrieve_file, mock_retrieve_dir):
        dag = DAG(
            dag_id="unit_tests_sftp_op_arg_checking",
            schedule=None,
            default_args={"start_date": DEFAULT_DATE},
        )
        # Exception should be raised if ssh_conn_id is not provided
        task_0 = SFTPOperator(
            task_id="test_sftp_0",
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=SFTPOperation.PUT,
            dag=dag,
        )
        with pytest.raises(AirflowException, match="Cannot operate without sftp_hook or ssh_conn_id."):
            task_0.execute(None)

        # use ssh_conn_id to create SSHHook
        task_1 = SFTPOperator(
            task_id="test_sftp_1",
            ssh_conn_id=TEST_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=SFTPOperation.PUT,
            dag=dag,
        )
        with contextlib.suppress(Exception):
            task_1.execute(None)
        assert task_1.sftp_hook.ssh_conn_id == TEST_CONN_ID

        task_2 = SFTPOperator(
            task_id="test_sftp_2",
            ssh_conn_id=TEST_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=SFTPOperation.PUT,
            dag=dag,
        )
        with contextlib.suppress(Exception):
            task_2.execute(None)
        assert task_2.sftp_hook.ssh_conn_id == TEST_CONN_ID

        task_3 = SFTPOperator(
            task_id="test_sftp_3",
            ssh_conn_id=TEST_CONN_ID,
            remote_host="remotehost",
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=SFTPOperation.PUT,
            dag=dag,
        )
        with contextlib.suppress(Exception):
            task_3.execute(None)
        assert task_3.sftp_hook.remote_host == "remotehost"

    def test_unequal_local_remote_file_paths(self):
        with pytest.raises(ValueError, match="1 paths in local_filepath != 2 paths in remote_filepath"):
            SFTPOperator(
                task_id="test_sftp_unequal_paths",
                local_filepath="/tmp/test",
                remote_filepath=["/tmp/test1", "/tmp/test2"],
            ).execute(None)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.retrieve_file")
    def test_non_deferrable_get_uses_sync_hook(self, mock_get):
        SFTPOperator(
            task_id="test_non_deferrable_get",
            sftp_hook=self.sftp_hook,
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation=SFTPOperation.GET,
            deferrable=False,
        ).execute(None)
        mock_get.assert_called_once_with("/tmp/remote", "/tmp/local")

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.retrieve_file")
    def test_str_filepaths_get(self, mock_get):
        local_filepath = "/tmp/test"
        remote_filepath = "/tmp/remotetest"
        SFTPOperator(
            task_id="test_str_to_list",
            sftp_hook=self.sftp_hook,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=SFTPOperation.GET,
        ).execute(None)
        assert mock_get.call_count == 1
        args, _ = mock_get.call_args_list[0]
        assert args == (remote_filepath, local_filepath)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.retrieve_file")
    def test_multiple_paths_get(self, mock_get):
        local_filepath = ["/tmp/ltest1", "/tmp/ltest2"]
        remote_filepath = ["/tmp/rtest1", "/tmp/rtest2"]
        sftp_op = SFTPOperator(
            task_id="test_multiple_paths_get",
            sftp_hook=self.sftp_hook,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=SFTPOperation.GET,
        )
        sftp_op.execute(None)
        assert mock_get.call_count == 2
        args0, _ = mock_get.call_args_list[0]
        args1, _ = mock_get.call_args_list[1]
        assert args0 == (remote_filepath[0], local_filepath[0])
        assert args1 == (remote_filepath[1], local_filepath[1])

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.retrieve_directory")
    def test_str_dirpaths_get(self, mock_get):
        local_dirpath = "/tmp_local"
        remote_dirpath = "/tmp"
        SFTPOperator(
            task_id="test_str_to_list",
            sftp_hook=self.sftp_hook,
            local_filepath=local_dirpath,
            remote_filepath=remote_dirpath,
            operation=SFTPOperation.GET,
        ).execute(None)
        assert mock_get.call_count == 1
        args, _ = mock_get.call_args_list[0]
        assert args == (remote_dirpath, local_dirpath)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.retrieve_directory_concurrently")
    def test_str_dirpaths_get_concurrently(self, mock_get):
        local_dirpath = "/tmp_local"
        remote_dirpath = "/tmp"
        SFTPOperator(
            task_id="test_str_to_list",
            sftp_hook=self.sftp_hook,
            local_filepath=local_dirpath,
            remote_filepath=remote_dirpath,
            operation=SFTPOperation.GET,
            concurrency=2,
        ).execute(None)
        assert mock_get.call_count == 1
        assert mock_get.call_args == mock.call(remote_dirpath, local_dirpath, workers=2, prefetch=True)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.store_file")
    def test_str_filepaths_put(self, mock_get):
        local_filepath = "/tmp/test"
        remote_filepath = "/tmp/remotetest"
        SFTPOperator(
            task_id="test_str_to_list",
            sftp_hook=self.sftp_hook,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=SFTPOperation.PUT,
        ).execute(None)
        assert mock_get.call_count == 1
        args, _ = mock_get.call_args_list[0]
        assert args == (remote_filepath, local_filepath)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.store_file")
    def test_multiple_paths_put(self, mock_put):
        local_filepath = ["/tmp/ltest1", "/tmp/ltest2"]
        remote_filepath = ["/tmp/rtest1", "/tmp/rtest2"]
        sftp_op = SFTPOperator(
            task_id="test_multiple_paths_get",
            sftp_hook=self.sftp_hook,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=SFTPOperation.PUT,
        )
        sftp_op.execute(None)
        assert mock_put.call_count == 2
        args0, _ = mock_put.call_args_list[0]
        args1, _ = mock_put.call_args_list[1]
        assert args0 == (remote_filepath[0], local_filepath[0])
        assert args1 == (remote_filepath[1], local_filepath[1])

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.store_directory")
    def test_str_dirpaths_put(self, mock_get):
        local_dirpath = "/tmp"
        remote_dirpath = "/tmp_remote"
        SFTPOperator(
            task_id="test_str_dirpaths_put",
            sftp_hook=self.sftp_hook,
            local_filepath=local_dirpath,
            remote_filepath=remote_dirpath,
            operation=SFTPOperation.PUT,
        ).execute(None)
        assert mock_get.call_count == 1
        args, _ = mock_get.call_args_list[0]
        assert args == (remote_dirpath, local_dirpath)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.store_directory_concurrently")
    def test_str_dirpaths_put_concurrently(self, mock_get):
        local_dirpath = "/tmp"
        remote_dirpath = "/tmp_remote"
        SFTPOperator(
            task_id="test_str_dirpaths_put",
            sftp_hook=self.sftp_hook,
            local_filepath=local_dirpath,
            remote_filepath=remote_dirpath,
            operation=SFTPOperation.PUT,
            concurrency=2,
        ).execute(None)
        assert mock_get.call_count == 1
        args, _ = mock_get.call_args_list[0]
        assert args == (remote_dirpath, local_dirpath)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.retrieve_file")
    def test_return_str_when_local_filepath_was_str(self, mock_get):
        local_filepath = "/tmp/ltest1"
        remote_filepath = "/tmp/rtest1"
        sftp_op = SFTPOperator(
            task_id="test_returns_str",
            sftp_hook=self.sftp_hook,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=SFTPOperation.GET,
        )
        return_value = sftp_op.execute(None)
        assert isinstance(return_value, str)
        assert return_value == local_filepath

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.delete_file")
    def test_str_filepaths_delete(self, mock_delete):
        remote_filepath = "/tmp/test"
        SFTPOperator(
            task_id="test_str_filepaths_delete",
            sftp_hook=self.sftp_hook,
            remote_filepath=remote_filepath,
            operation=SFTPOperation.DELETE,
        ).execute(None)
        assert mock_delete.call_count == 1
        args, _ = mock_delete.call_args_list[0]
        assert args == (remote_filepath,)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.delete_file")
    def test_multiple_filepaths_delete(self, mock_delete):
        remote_filepath = ["/tmp/rtest1", "/tmp/rtest2"]
        SFTPOperator(
            task_id="test_multiple_filepaths_delete",
            sftp_hook=self.sftp_hook,
            remote_filepath=remote_filepath,
            operation=SFTPOperation.DELETE,
        ).execute(None)
        assert mock_delete.call_count == 2
        args0, _ = mock_delete.call_args_list[0]
        args1, _ = mock_delete.call_args_list[1]
        assert args0 == (remote_filepath[0],)
        assert args1 == (remote_filepath[1],)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.delete_directory")
    def test_str_dirpaths_delete(self, mock_delete):
        remote_filepath = "/tmp"
        SFTPOperator(
            task_id="test_str_dirpaths_delete",
            sftp_hook=self.sftp_hook,
            remote_filepath=remote_filepath,
            operation=SFTPOperation.DELETE,
        ).execute(None)
        assert mock_delete.call_count == 1
        args, _ = mock_delete.call_args_list[0]
        assert args == (remote_filepath,)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.delete_file")
    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.isdir")
    def test_delete_missing_file_warns(self, mock_isdir, mock_delete, caplog):
        mock_isdir.return_value = False
        mock_delete.side_effect = FileNotFoundError("missing")
        remote_filepath = "/tmp/missing"
        sftp_op = SFTPOperator(
            task_id="test_missing_file_delete_warns",
            sftp_hook=self.sftp_hook,
            remote_filepath=remote_filepath,
            operation=SFTPOperation.DELETE,
        )

        with caplog.at_level(logging.WARNING):
            sftp_op.execute(None)

        assert "does not exist. Skipping delete." in caplog.text

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.delete_file")
    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.isdir")
    def test_delete_permission_error_raises(self, mock_isdir, mock_delete):
        mock_isdir.return_value = False
        mock_delete.side_effect = PermissionError(errno.EACCES, "denied")
        remote_filepath = "/tmp/protected"

        with pytest.raises(AirflowException):
            SFTPOperator(
                task_id="test_permission_error_delete_raises",
                sftp_hook=self.sftp_hook,
                remote_filepath=remote_filepath,
                operation=SFTPOperation.DELETE,
            ).execute(None)

    @mock.patch("airflow.providers.sftp.operators.sftp.SFTPHook.delete_file")
    def test_local_filepath_exists_error_delete(self, mock_delete):
        local_filepath = "/tmp"
        remote_filepath = "/tmp_remote"
        with pytest.raises(ValueError, match="local_filepath should not be provided for delete operation"):
            SFTPOperator(
                task_id="test_local_filepath_exists_error_delete",
                sftp_hook=self.sftp_hook,
                local_filepath=local_filepath,
                remote_filepath=remote_filepath,
                operation=SFTPOperation.DELETE,
            ).execute(None)

    @pytest.mark.parametrize(
        ("operation", "expected"),
        TEST_GET_PUT_PARAMS,
    )
    @mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn", spec=paramiko.SSHClient)
    @mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_connection", spec=Connection)
    def test_extract_ssh_conn_id(self, get_connection, get_conn, operation, expected):
        get_connection.return_value = Connection(
            conn_id="sftp_conn_id",
            conn_type="sftp",
            host="remotehost",
            port=22,
        )

        dag_id = "sftp_dag"
        task_id = "sftp_task"

        task = SFTPOperator(
            task_id=task_id,
            ssh_conn_id="sftp_conn_id",
            dag=DAG(dag_id, schedule=None),
            start_date=timezone.utcnow(),
            local_filepath="/path/local",
            remote_filepath="/path/remote",
            operation=operation,
        )
        lineage = task.get_openlineage_facets_on_start()

        assert lineage.inputs == expected[0]
        assert lineage.outputs == expected[1]

    @pytest.mark.parametrize(
        ("operation", "expected"),
        TEST_GET_PUT_PARAMS,
    )
    @mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn", spec=paramiko.SSHClient)
    @mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_connection", spec=Connection)
    def test_extract_sftp_hook(self, get_connection, get_conn, operation, expected):
        get_connection.return_value = Connection(
            conn_id="sftp_conn_id",
            conn_type="sftp",
            host="remotehost",
            port=22,
        )

        dag_id = "sftp_dag"
        task_id = "sftp_task"

        task = SFTPOperator(
            task_id=task_id,
            sftp_hook=SFTPHook(ssh_conn_id="sftp_conn_id"),
            dag=DAG(dag_id, schedule=None),
            start_date=timezone.utcnow(),
            local_filepath="/path/local",
            remote_filepath="/path/remote",
            operation=operation,
        )
        lineage = task.get_openlineage_facets_on_start()

        assert lineage.inputs == expected[0]
        assert lineage.outputs == expected[1]

    def test_deferrable_get_defers(self):
        operator = SFTPOperator(
            task_id="test_deferrable_get",
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation=SFTPOperation.GET,
            create_intermediate_dirs=True,
            remote_host="sftp.example.com",
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(None)
        trigger = exc.value.trigger
        assert isinstance(trigger, SFTPTransferTrigger)
        assert trigger.ssh_conn_id == "sftp_default"
        assert trigger.local_filepath == "/tmp/local"
        assert trigger.remote_filepath == "/tmp/remote"
        assert trigger.operation == SFTPOperation.GET
        assert trigger.create_intermediate_dirs is True
        assert trigger.remote_host == "sftp.example.com"
        assert exc.value.method_name == "execute_complete"

    def test_deferrable_get_multiple_files_defers(self):
        local_filepath = ["/tmp/a", "/tmp/b"]
        remote_filepath = ["/remote/a", "/remote/b"]
        operator = SFTPOperator(
            task_id="test_deferrable_get_multi",
            ssh_conn_id="sftp_default",
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=SFTPOperation.GET,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(None)
        assert exc.value.trigger.local_filepath == local_filepath
        assert exc.value.trigger.remote_filepath == remote_filepath

    def test_deferrable_put_defers(self):
        operator = SFTPOperator(
            task_id="test_deferrable_put",
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation=SFTPOperation.PUT,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(None)
        assert isinstance(exc.value.trigger, SFTPTransferTrigger)
        assert exc.value.trigger.operation == SFTPOperation.PUT

    def test_deferrable_uses_hook_ssh_conn_id(self):
        operator = SFTPOperator(
            task_id="test_deferrable_hook_conn",
            sftp_hook=self.sftp_hook,
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation=SFTPOperation.GET,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(None)
        assert exc.value.trigger.ssh_conn_id == "ssh_default"

    def test_deferrable_delete_raises(self):
        operator = SFTPOperator(
            task_id="test_deferrable_delete",
            ssh_conn_id="sftp_default",
            remote_filepath="/tmp/remote",
            operation=SFTPOperation.DELETE,
            deferrable=True,
        )
        with pytest.raises(ValueError, match="does not support the delete operation"):
            operator.execute(None)

    def test_deferrable_concurrency_raises(self):
        operator = SFTPOperator(
            task_id="test_deferrable_concurrency",
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation=SFTPOperation.GET,
            concurrency=2,
            deferrable=True,
        )
        with pytest.raises(ValueError, match="does not support concurrency greater than 1"):
            operator.execute(None)

    def test_deferrable_missing_conn_id_raises(self):
        operator = SFTPOperator(
            task_id="test_deferrable_no_conn",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation=SFTPOperation.GET,
            deferrable=True,
        )
        with pytest.raises(ValueError, match="requires ssh_conn_id"):
            operator.execute(None)

    @mock.patch("airflow.providers.sftp.operators.sftp.os.path.isdir", return_value=True)
    def test_deferrable_directory_put_raises(self, mock_isdir):
        operator = SFTPOperator(
            task_id="test_deferrable_dir",
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local_dir",
            remote_filepath="/tmp/remote_dir",
            operation=SFTPOperation.PUT,
            deferrable=True,
        )
        with pytest.raises(ValueError, match="does not support directory transfers"):
            operator.execute(None)

    def test_execute_complete_success(self):
        operator = SFTPOperator(
            task_id="test_execute_complete_success",
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
        )
        assert (
            operator.execute_complete({}, {"status": "success", "local_filepath": "/tmp/local"})
            == "/tmp/local"
        )

    def test_execute_complete_error_raises(self):
        operator = SFTPOperator(
            task_id="test_execute_complete_error",
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
        )
        with pytest.raises(ValueError, match="transfer failed"):
            operator.execute_complete({}, {"status": "error", "message": "transfer failed"})

    def test_execute_complete_missing_event_raises(self):
        operator = SFTPOperator(
            task_id="test_execute_complete_missing",
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
        )
        with pytest.raises(ValueError, match="No event received"):
            operator.execute_complete({}, None)

    def test_execute_complete_unexpected_status_raises(self):
        operator = SFTPOperator(
            task_id="test_execute_complete_unexpected",
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
        )
        with pytest.raises(ValueError, match="Unexpected event status"):
            operator.execute_complete({}, {"status": "running"})

    def test_deferrable_logs_triggerer_warning(self, caplog):
        operator = SFTPOperator(
            task_id="test_deferrable_warning",
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation=SFTPOperation.GET,
            deferrable=True,
        )
        with caplog.at_level(logging.WARNING), pytest.raises(TaskDeferred):
            operator.execute(None)
        assert "Triggerer host" in caplog.text
