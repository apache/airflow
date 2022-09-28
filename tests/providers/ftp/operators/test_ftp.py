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

import os
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.ftp.operators.ftp import FTPOperation, FTPOperator
from airflow.utils import timezone
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2017, 1, 1)
TEST_CONN_ID = "conn_id_for_testing"


class TestFTPOperator:
    def setup_method(self):
        hook = FTPHook()
        hook.no_host_key_check = True
        self.hook = hook
        self.test_local_dir = "/tmp"
        self.test_local_dir_int = "/tmp/interdir"
        self.test_remote_dir = "/ftphome"
        self.test_remote_dir_int = "/ftphome/interdir"
        self.test_local_filename = 'test_local_file'
        self.test_remote_filename = 'test_remote_file'
        self.test_local_filepath = f'{self.test_local_dir}/{self.test_local_filename}'
        self.test_remote_filepath = f'{self.test_remote_dir}/{self.test_remote_filename}'
        self.test_local_filepath_int_dir = f'{self.test_local_dir_int}/{self.test_local_filename}'
        self.test_remote_filepath_int_dir = f'{self.test_remote_dir_int}/{self.test_remote_filename}'

    def teardown_method(self):
        if os.path.exists(self.test_local_filepath):
            os.remove(self.test_local_filepath)
        if os.path.exists(self.test_local_filepath_int_dir):
            os.remove(self.test_local_filepath_int_dir)
        if os.path.exists(self.test_local_dir_int):
            os.rmdir(self.test_local_dir_int)
        if os.path.exists(self.test_remote_filepath):
            os.remove(self.test_remote_filepath)
        if os.path.exists(self.test_remote_filepath_int_dir):
            os.remove(self.test_remote_filepath_int_dir)
        if os.path.exists(self.test_remote_dir_int):
            os.rmdir(self.test_remote_dir_int)

    def test_file_transfer_put(self, create_task_instance_of_operator):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )

        # create a test file locally
        with open(self.test_local_filepath, 'wb') as file:
            file.write(test_local_file_content)

        ti = create_task_instance_of_operator(
            FTPOperator,
            dag_id="unit_tests_ftp_op_file_transfer_put",
            execution_date=timezone.utcnow(),
            task_id="put_test_task",
            ftp_hook=self.hook,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.PUT,
        )

        ti.run()

        self.hook.retrieve_file(
            remote_full_path=self.test_remote_filepath, local_full_path_or_buffer="test_file"
        )
        assert open("test_file", "rb").read().strip() == test_local_file_content

    def test_file_transfer_intermediate_dir_error_put(self, create_task_instance_of_operator):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )
        # create a test file locally
        with open(self.test_local_filepath, 'wb') as file:
            file.write(test_local_file_content)

        # Try to put test file to remote. This should raise an error with
        # "Failed to change directory" as the directory does not exist.
        ti = create_task_instance_of_operator(
            FTPOperator,
            dag_id="unit_tests_ftp_op_file_transfer_no_intermediate_dir_error_put",
            execution_date=timezone.utcnow(),
            task_id="test_ftp",
            ftp_hook=self.hook,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath_int_dir,
            operation=FTPOperation.PUT,
            create_intermediate_dirs=False,
        )
        with pytest.raises(Exception) as ctx:
            ti.run()
        assert 'Failed to change directory' in str(ctx.value)

    def test_file_transfer_with_intermediate_dir_put(self, create_task_instance_of_operator):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )
        # create a test file locally
        with open(self.test_local_filepath, 'wb') as file:
            file.write(test_local_file_content)

        ti = create_task_instance_of_operator(
            FTPOperator,
            dag_id="unit_tests_ftp_op_file_transfer_with_intermediate_dir_put",
            execution_date=timezone.utcnow(),
            task_id="test_ftp",
            ftp_hook=self.hook,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath_int_dir,
            operation=FTPOperation.PUT,
            create_intermediate_dirs=True,
        )

        ti.run()

        self.hook.retrieve_file(
            remote_full_path=self.test_remote_filepath_int_dir, local_full_path_or_buffer="test_file"
        )
        assert open("test_file", "rb").read().strip() == test_local_file_content

    def test_file_transfer_get(self, create_task_instance_of_operator):
        test_remote_file_content = (
            b"This is remote file content \n which is also multiline "
            b"another line here \n this is last line. EOF"
        )
        from io import BytesIO

        self.hook.store_file(
            remote_full_path=self.test_remote_filepath,
            local_full_path_or_buffer=BytesIO(test_remote_file_content),
        )

        ti = create_task_instance_of_operator(
            FTPOperator,
            dag_id="unit_tests_ftp_op_file_transfer_get",
            execution_date=timezone.utcnow(),
            task_id="test_ftp",
            ftp_hook=self.hook,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.GET,
        )

        ti.run()
        # Test the received content.
        with open(self.test_local_filepath, "rb") as file:
            content_received = file.read()
        assert content_received.strip() == test_remote_file_content

    def test_file_transfer_no_intermediate_dir_error_get(self, create_task_instance_of_operator):
        test_remote_file_content = (
            b"This is remote file content \n which is also multiline "
            b"another line here \n this is last line. EOF"
        )
        from io import BytesIO

        self.hook.store_file(
            remote_full_path=self.test_remote_filepath,
            local_full_path_or_buffer=BytesIO(test_remote_file_content),
        )

        ti = create_task_instance_of_operator(
            FTPOperator,
            dag_id="unit_tests_ftp_op_file_transfer_get",
            execution_date=timezone.utcnow(),
            task_id="test_ftp",
            ftp_hook=self.hook,
            local_filepath=self.test_local_filepath_int_dir,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.GET,
        )
        # This should raise an error with "No such file" as the directory
        # does not exist.
        with pytest.raises(Exception) as ctx:
            ti.run()
        assert 'No such file' in str(ctx.value)

    def test_file_transfer_intermediate_dir_get(self, create_task_instance_of_operator):
        test_remote_file_content = (
            b"This is remote file content \n which is also multiline "
            b"another line here \n this is last line. EOF"
        )
        from io import BytesIO

        self.hook.store_file(
            remote_full_path=self.test_remote_filepath,
            local_full_path_or_buffer=BytesIO(test_remote_file_content),
        )

        ti = create_task_instance_of_operator(
            FTPOperator,
            dag_id="unit_tests_ftp_op_file_transfer_get",
            execution_date=timezone.utcnow(),
            task_id="test_ftp",
            ftp_hook=self.hook,
            local_filepath=self.test_local_filepath_int_dir,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.GET,
            create_intermediate_dirs=True,
        )

        ti.run()

        with open(self.test_local_filepath_int_dir, "rb") as file:
            content_received = file.read()
        assert content_received.strip() == test_remote_file_content

    @mock.patch('airflow.providers.ftp.operators.ftp.FTPHook.retrieve_file')
    def test_multiple_paths_get(self, mock_get):
        local_filepath = ['/tmp/ltest1', '/tmp/ltest2']
        remote_filepath = ['/tmp/rtest1', '/tmp/rtest2']
        ftp_op = FTPOperator(
            task_id='test_multiple_paths_get',
            ftp_hook=self.hook,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=FTPOperation.GET,
        )
        ftp_op.execute(None)
        assert mock_get.call_count == 2
        args0, _ = mock_get.call_args_list[0]
        args1, _ = mock_get.call_args_list[1]
        assert args0 == (remote_filepath[0], local_filepath[0])
        assert args1 == (remote_filepath[1], local_filepath[1])

    @mock.patch('airflow.providers.ftp.operators.ftp.FTPHook.store_file')
    def test_multiple_paths_put(self, mock_put):
        local_filepath = ['/tmp/ltest1', '/tmp/ltest2']
        remote_filepath = ['/tmp/rtest1', '/tmp/rtest2']
        ftp_op = FTPOperator(
            task_id='test_multiple_paths_get',
            ftp_hook=self.hook,
            local_filepath=local_filepath,
            remote_filepath=remote_filepath,
            operation=FTPOperation.PUT,
        )
        ftp_op.execute(None)
        assert mock_put.call_count == 2
        args0, _ = mock_put.call_args_list[0]
        args1, _ = mock_put.call_args_list[1]
        assert args0 == (remote_filepath[0], local_filepath[0])
        assert args1 == (remote_filepath[1], local_filepath[1])

    def test_arg_checking(self):
        dag = DAG(dag_id="unit_tests_ftp_op_arg_checking", default_args={"start_date": DEFAULT_DATE})
        # Exception should be raised if neither ftp_hook nor ftp_conn_id is provided
        with pytest.raises(AirflowException, match="Cannot operate without ftp_hook or ftp_conn_id."):
            task_0 = FTPOperator(
                task_id="test_ftp_0",
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=FTPOperation.PUT,
                dag=dag,
            )
            task_0.execute(None)

        # if ftp_hook is invalid/not provided, use ftp_conn_id to create FTPHook
        task_1 = FTPOperator(
            task_id="test_ftp_1",
            ftp_hook="string_rather_than_FTPHook",  # type: ignore
            ftp_conn_id=TEST_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.PUT,
            dag=dag,
        )
        try:
            task_1.execute(None)
        except Exception:
            pass
        assert task_1.ftp_hook.ftp_conn_id == TEST_CONN_ID

        task_2 = FTPOperator(
            task_id="test_ftp_2",
            ftp_conn_id=TEST_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.PUT,
            dag=dag,
        )
        try:
            task_2.execute(None)
        except Exception:
            pass
        assert task_2.ftp_hook.ftp_conn_id == TEST_CONN_ID

        # if both valid ftp_hook and ftp_conn_id are provided, ignore ftp_conn_id
        task_3 = FTPOperator(
            task_id="test_ftp_3",
            ftp_hook=self.hook,
            ftp_conn_id=TEST_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=FTPOperation.PUT,
            dag=dag,
        )
        try:
            task_3.execute(None)
        except Exception:
            pass
        assert task_3.ftp_hook.ftp_conn_id == self.hook.ftp_conn_id

        # Exception should be raised if operation is invalid
        with pytest.raises(TypeError, match="Unsupported operation value invalid_operation, "):
            task_4 = FTPOperator(
                task_id="test_ftp_4",
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation='invalid_operation',
                dag=dag,
            )
            task_4.execute(None)

    def test_unequal_local_remote_file_paths(self):
        with pytest.raises(ValueError):
            FTPOperator(
                task_id='test_ftp_unequal_paths',
                local_filepath='/tmp/test',
                remote_filepath=['/tmp/test1', '/tmp/test2'],
            )

        with pytest.raises(ValueError):
            FTPOperator(
                task_id='test_ftp_unequal_paths',
                local_filepath=['/tmp/test1', '/tmp/test2'],
                remote_filepath='/tmp/test1',
            )


# class TestFTPSOperator(TestFTPOperator):
#     def setup_method(self):
#         self.hook = FTPSHook('ssh_default')
#         super().setup_method()
