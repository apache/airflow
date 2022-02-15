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
from base64 import b64encode
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperation, SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils import timezone
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars

DEFAULT_DATE = datetime(2017, 1, 1)
TEST_CONN_ID = "conn_id_for_testing"


class TestSFTPOperator:
    def setup_method(self):
        from airflow.providers.ssh.hooks.ssh import SSHHook

        hook = SSHHook(ssh_conn_id='ssh_default')
        hook.no_host_key_check = True
        self.hook = hook
        self.test_dir = "/tmp"
        self.test_local_dir = "/tmp/tmp2"
        self.test_remote_dir = "/tmp/tmp1"
        self.test_local_filename = 'test_local_file'
        self.test_remote_filename = 'test_remote_file'
        self.test_local_filepath = f'{self.test_dir}/{self.test_local_filename}'
        # Local Filepath with Intermediate Directory
        self.test_local_filepath_int_dir = f'{self.test_local_dir}/{self.test_local_filename}'
        self.test_remote_filepath = f'{self.test_dir}/{self.test_remote_filename}'
        # Remote Filepath with Intermediate Directory
        self.test_remote_filepath_int_dir = f'{self.test_remote_dir}/{self.test_remote_filename}'

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

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_pickle_file_transfer_put(self, dag_maker):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )
        # create a test file locally
        with open(self.test_local_filepath, 'wb') as file:
            file.write(test_local_file_content)

        with dag_maker(dag_id="unit_tests_sftp_op_pickle_file_transfer_put", start_date=DEFAULT_DATE):
            SFTPOperator(  # Put test file to remote.
                task_id="put_test_task",
                ssh_hook=self.hook,
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

        tis = {ti.task_id: ti for ti in dag_maker.create_dagrun().task_instances}
        tis["put_test_task"].run()
        tis["check_file_task"].run()

        pulled = tis["check_file_task"].xcom_pull(task_ids="check_file_task", key='return_value')
        assert pulled.strip() == test_local_file_content

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_file_transfer_no_intermediate_dir_error_put(self, create_task_instance_of_operator):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )
        # create a test file locally
        with open(self.test_local_filepath, 'wb') as file:
            file.write(test_local_file_content)

        # Try to put test file to remote. This should raise an error with
        # "No such file" as the directory does not exist.
        ti2 = create_task_instance_of_operator(
            SFTPOperator,
            dag_id="unit_tests_sftp_op_file_transfer_no_intermediate_dir_error_put",
            execution_date=timezone.utcnow(),
            task_id="test_sftp",
            ssh_hook=self.hook,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath_int_dir,
            operation=SFTPOperation.PUT,
            create_intermediate_dirs=False,
        )
        with pytest.raises(Exception) as ctx:
            ti2.run()
        assert 'No such file' in str(ctx.value)

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_file_transfer_with_intermediate_dir_put(self, dag_maker):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )
        # create a test file locally
        with open(self.test_local_filepath, 'wb') as file:
            file.write(test_local_file_content)

        with dag_maker(dag_id="unit_tests_sftp_op_file_transfer_with_intermediate_dir_put"):
            SFTPOperator(  # Put test file to remote.
                task_id="test_sftp",
                ssh_hook=self.hook,
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

        dagrun = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        tis = {ti.task_id: ti for ti in dagrun.task_instances}
        tis["test_sftp"].run()
        tis["test_check_file"].run()

        pulled = tis["test_check_file"].xcom_pull(task_ids='test_check_file', key='return_value')
        assert pulled.strip() == test_local_file_content

    @conf_vars({('core', 'enable_xcom_pickling'): 'False'})
    def test_json_file_transfer_put(self, dag_maker):
        test_local_file_content = (
            b"This is local file content \n which is multiline "
            b"continuing....with other character\nanother line here \n this is last line"
        )
        # create a test file locally
        with open(self.test_local_filepath, 'wb') as file:
            file.write(test_local_file_content)

        with dag_maker(dag_id="unit_tests_sftp_op_json_file_transfer_put"):
            SFTPOperator(  # Put test file to remote.
                task_id="put_test_task",
                ssh_hook=self.hook,
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

        dagrun = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        tis = {ti.task_id: ti for ti in dagrun.task_instances}
        tis["put_test_task"].run()
        tis["check_file_task"].run()

        pulled = tis["check_file_task"].xcom_pull(task_ids="check_file_task", key='return_value')
        assert pulled.strip() == b64encode(test_local_file_content).decode('utf-8')

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_pickle_file_transfer_get(self, dag_maker):
        test_remote_file_content = (
            "This is remote file content \n which is also multiline "
            "another line here \n this is last line. EOF"
        )

        with dag_maker(dag_id="unit_tests_sftp_op_pickle_file_transfer_get"):
            SSHOperator(  # Create a test file on remote.
                task_id="test_create_file",
                ssh_hook=self.hook,
                command=f"echo '{test_remote_file_content}' > {self.test_remote_filepath}",
                do_xcom_push=True,
            )
            SFTPOperator(  # Get remote file to local.
                task_id="test_sftp",
                ssh_hook=self.hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.GET,
            )

        for ti in dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances:
            ti.run()

        # Test the received content.
        with open(self.test_local_filepath) as file:
            content_received = file.read()
        assert content_received.strip() == test_remote_file_content

    @conf_vars({('core', 'enable_xcom_pickling'): 'False'})
    def test_json_file_transfer_get(self, dag_maker):
        test_remote_file_content = (
            "This is remote file content \n which is also multiline "
            "another line here \n this is last line. EOF"
        )

        with dag_maker(dag_id="unit_tests_sftp_op_json_file_transfer_get"):
            SSHOperator(  # Create a test file on remote.
                task_id="test_create_file",
                ssh_hook=self.hook,
                command=f"echo '{test_remote_file_content}' > {self.test_remote_filepath}",
                do_xcom_push=True,
            )
            SFTPOperator(  # Get remote file to local.
                task_id="test_sftp",
                ssh_hook=self.hook,
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.GET,
            )

        for ti in dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances:
            ti.run()

        # Test the received content.
        content_received = None
        with open(self.test_local_filepath) as file:
            content_received = file.read()
        assert content_received.strip() == test_remote_file_content.encode('utf-8').decode('utf-8')

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_file_transfer_no_intermediate_dir_error_get(self, dag_maker):
        test_remote_file_content = (
            "This is remote file content \n which is also multiline "
            "another line here \n this is last line. EOF"
        )

        with dag_maker(dag_id="unit_tests_sftp_op_file_transfer_no_intermediate_dir_error_get"):
            SSHOperator(  # Create a test file on remote.
                task_id="test_create_file",
                ssh_hook=self.hook,
                command=f"echo '{test_remote_file_content}' > {self.test_remote_filepath}",
                do_xcom_push=True,
            )
            SFTPOperator(  # Try to GET test file from remote.
                task_id="test_sftp",
                ssh_hook=self.hook,
                local_filepath=self.test_local_filepath_int_dir,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.GET,
            )

        ti1, ti2 = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances
        ti1.run()

        # This should raise an error with "No such file" as the directory
        # does not exist.
        with pytest.raises(Exception) as ctx:
            ti2.run()
        assert 'No such file' in str(ctx.value)

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_file_transfer_with_intermediate_dir_error_get(self, dag_maker):
        test_remote_file_content = (
            "This is remote file content \n which is also multiline "
            "another line here \n this is last line. EOF"
        )

        with dag_maker(dag_id="unit_tests_sftp_op_file_transfer_with_intermediate_dir_error_get"):
            SSHOperator(  # Create a test file on remote.
                task_id="test_create_file",
                ssh_hook=self.hook,
                command=f"echo '{test_remote_file_content}' > {self.test_remote_filepath}",
                do_xcom_push=True,
            )
            SFTPOperator(  # Get remote file to local.
                task_id="test_sftp",
                ssh_hook=self.hook,
                local_filepath=self.test_local_filepath_int_dir,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.GET,
                create_intermediate_dirs=True,
            )

        for ti in dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances:
            ti.run()

        # Test the received content.
        content_received = None
        with open(self.test_local_filepath_int_dir) as file:
            content_received = file.read()
        assert content_received.strip() == test_remote_file_content

    @mock.patch.dict('os.environ', {'AIRFLOW_CONN_' + TEST_CONN_ID.upper(): "ssh://test_id@localhost"})
    def test_arg_checking(self):
        dag = DAG(dag_id="unit_tests_sftp_op_arg_checking", default_args={"start_date": DEFAULT_DATE})
        # Exception should be raised if neither ssh_hook nor ssh_conn_id is provided
        with pytest.raises(AirflowException, match="Cannot operate without ssh_hook or ssh_conn_id."):
            task_0 = SFTPOperator(
                task_id="test_sftp_0",
                local_filepath=self.test_local_filepath,
                remote_filepath=self.test_remote_filepath,
                operation=SFTPOperation.PUT,
                dag=dag,
            )
            task_0.execute(None)

        # if ssh_hook is invalid/not provided, use ssh_conn_id to create SSHHook
        task_1 = SFTPOperator(
            task_id="test_sftp_1",
            ssh_hook="string_rather_than_SSHHook",  # invalid ssh_hook
            ssh_conn_id=TEST_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=SFTPOperation.PUT,
            dag=dag,
        )
        try:
            task_1.execute(None)
        except Exception:
            pass
        assert task_1.ssh_hook.ssh_conn_id == TEST_CONN_ID

        task_2 = SFTPOperator(
            task_id="test_sftp_2",
            ssh_conn_id=TEST_CONN_ID,  # no ssh_hook provided
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=SFTPOperation.PUT,
            dag=dag,
        )
        try:
            task_2.execute(None)
        except Exception:
            pass
        assert task_2.ssh_hook.ssh_conn_id == TEST_CONN_ID

        # if both valid ssh_hook and ssh_conn_id are provided, ignore ssh_conn_id
        task_3 = SFTPOperator(
            task_id="test_sftp_3",
            ssh_hook=self.hook,
            ssh_conn_id=TEST_CONN_ID,
            local_filepath=self.test_local_filepath,
            remote_filepath=self.test_remote_filepath,
            operation=SFTPOperation.PUT,
            dag=dag,
        )
        try:
            task_3.execute(None)
        except Exception:
            pass
        assert task_3.ssh_hook.ssh_conn_id == self.hook.ssh_conn_id
