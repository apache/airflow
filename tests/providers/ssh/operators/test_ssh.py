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

import unittest.mock
from base64 import b64encode

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars

TEST_DAG_ID = 'unit_tests_ssh_test_op'
TEST_CONN_ID = "conn_id_for_testing"
DEFAULT_TIMEOUT = 10
CONN_TIMEOUT = 5
CMD_TIMEOUT = 7
TIMEOUT = 12
DEFAULT_DATE = datetime(2017, 1, 1)
COMMAND = "echo -n airflow"
COMMAND_WITH_SUDO = "sudo " + COMMAND


class SSHClientSideEffect:
    def __init__(self, hook):
        self.hook = hook

    def __call__(self):
        self.return_value = self.hook.get_conn()
        return self.return_value


class TestSSHOperator:
    def setup_method(self):
        from airflow.providers.ssh.hooks.ssh import SSHHook

        hook = SSHHook(ssh_conn_id='ssh_default', banner_timeout=100)
        hook.no_host_key_check = True
        self.dag = DAG('ssh_test', default_args={'start_date': DEFAULT_DATE})
        self.hook = hook

    def test_hook_created_correctly_with_timeout(self):
        timeout = 20
        ssh_id = "ssh_default"
        with self.dag:
            task = SSHOperator(
                task_id="test",
                command=COMMAND,
                timeout=timeout,
                ssh_conn_id="ssh_default",
                banner_timeout=100,
            )
        task.execute(None)
        assert timeout == task.ssh_hook.conn_timeout
        assert ssh_id == task.ssh_hook.ssh_conn_id

    def test_hook_created_correctly(self):
        conn_timeout = 20
        cmd_timeout = 45
        ssh_id = 'ssh_default'
        with self.dag:
            task = SSHOperator(
                task_id="test",
                command=COMMAND,
                conn_timeout=conn_timeout,
                cmd_timeout=cmd_timeout,
                ssh_conn_id="ssh_default",
                banner_timeout=100,
            )
        task.execute(None)
        assert conn_timeout == task.ssh_hook.conn_timeout
        assert ssh_id == task.ssh_hook.ssh_conn_id

    @conf_vars({('core', 'enable_xcom_pickling'): 'False'})
    def test_json_command_execution(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            SSHOperator,
            dag_id="unit_tests_ssh_test_op_json_command_execution",
            task_id="test",
            ssh_hook=self.hook,
            command=COMMAND,
            do_xcom_push=True,
            banner_timeout=100,
        )
        ti.run()
        assert ti.duration is not None
        assert ti.xcom_pull(task_ids='test', key='return_value') == b64encode(b'airflow').decode('utf-8')

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_pickle_command_execution(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            SSHOperator,
            dag_id="unit_tests_ssh_test_op_pickle_command_execution",
            task_id="test",
            ssh_hook=self.hook,
            command=COMMAND,
            do_xcom_push=True,
            banner_timeout=100,
        )
        ti.run()
        assert ti.duration is not None
        assert ti.xcom_pull(task_ids='test', key='return_value') == b'airflow'

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_command_execution_with_env(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            SSHOperator,
            dag_id="unit_tests_ssh_test_op_command_execution_with_env",
            task_id="test",
            ssh_hook=self.hook,
            command=COMMAND,
            do_xcom_push=True,
            environment={'TEST': 'value'},
            banner_timeout=100,
        )
        ti.run()
        assert ti.duration is not None
        assert ti.xcom_pull(task_ids='test', key='return_value') == b'airflow'

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_no_output_command(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            SSHOperator,
            dag_id="unit_tests_ssh_test_op_no_output_command",
            task_id="test",
            ssh_hook=self.hook,
            command="sleep 1",
            do_xcom_push=True,
            banner_timeout=100,
        )
        ti.run()
        assert ti.duration is not None
        assert ti.xcom_pull(task_ids='test', key='return_value') == b''

    @unittest.mock.patch('os.environ', {'AIRFLOW_CONN_' + TEST_CONN_ID.upper(): "ssh://test_id@localhost"})
    def test_arg_checking(self):
        # Exception should be raised if neither ssh_hook nor ssh_conn_id is provided.
        task_0 = SSHOperator(task_id="test", command=COMMAND, timeout=TIMEOUT, dag=self.dag)
        with pytest.raises(AirflowException, match="Cannot operate without ssh_hook or ssh_conn_id."):
            task_0.execute(None)

        # If ssh_hook is invalid/not provided, use ssh_conn_id to create SSHHook.
        task_1 = SSHOperator(
            task_id="test_1",
            ssh_hook="string_rather_than_SSHHook",  # Invalid ssh_hook.
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
            timeout=TIMEOUT,
            dag=self.dag,
            banner_timeout=100,
        )
        try:
            task_1.execute(None)
        except Exception:
            pass
        assert task_1.ssh_hook.ssh_conn_id == TEST_CONN_ID

        task_2 = SSHOperator(
            task_id="test_2",
            ssh_conn_id=TEST_CONN_ID,  # No ssh_hook provided.
            command=COMMAND,
            timeout=TIMEOUT,
            dag=self.dag,
            banner_timeout=100,
        )
        try:
            task_2.execute(None)
        except Exception:
            pass
        assert task_2.ssh_hook.ssh_conn_id == TEST_CONN_ID

        # If both valid ssh_hook and ssh_conn_id are provided, ignore ssh_conn_id.
        task_3 = SSHOperator(
            task_id="test_3",
            ssh_hook=self.hook,
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
            timeout=TIMEOUT,
            dag=self.dag,
            banner_timeout=100,
        )
        task_3.execute(None)
        assert task_3.ssh_hook.ssh_conn_id == self.hook.ssh_conn_id
        # If remote_host was specified, ensure it is used
        task_4 = SSHOperator(
            task_id="test_4",
            ssh_hook=self.hook,
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
            timeout=TIMEOUT,
            dag=self.dag,
            remote_host='operator_remote_host',
            banner_timeout=100,
        )
        try:
            task_4.execute(None)
        except Exception:
            pass
        assert task_4.ssh_hook.ssh_conn_id == self.hook.ssh_conn_id
        assert task_4.ssh_hook.remote_host == 'operator_remote_host'

    @pytest.mark.parametrize(
        "command, get_pty_in, get_pty_out",
        [
            (COMMAND, False, False),
            (COMMAND, True, True),
            (COMMAND_WITH_SUDO, False, True),
            (COMMAND_WITH_SUDO, True, True),
            (None, True, True),
        ],
    )
    def test_get_pyt_set_correctly(self, command, get_pty_in, get_pty_out):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=command,
            conn_timeout=TIMEOUT,
            cmd_timeout=TIMEOUT,
            get_pty=get_pty_in,
            dag=self.dag,
            banner_timeout=100,
        )
        if command is None:
            with pytest.raises(AirflowException) as ctx:
                task.execute(None)
            assert str(ctx.value) == "SSH operator error: SSH command not specified. Aborting."
        else:
            task.execute(None)
            assert task.get_pty == get_pty_out

    def test_ssh_client_managed_correctly(self):
        # Ensure ssh_client gets created once
        # Ensure connection gets closed once
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command="ls",
            dag=self.dag,
            banner_timeout=100,
        )

        se = SSHClientSideEffect(self.hook)
        with unittest.mock.patch.object(task, 'get_ssh_client') as mock_get, unittest.mock.patch(
            'paramiko.client.SSHClient.close'
        ) as mock_close:
            mock_get.side_effect = se
            task.execute()
            mock_get.assert_called_once()
            mock_close.assert_called_once()

    def test_one_ssh_client_many_commands(self):
        # Ensure we can run multiple commands with one client
        many_commands = ['ls', 'date', 'pwd']

        class CustomSSHOperator(SSHOperator):
            def execute(self, context=None):
                success = False
                with self.get_ssh_client() as ssh_client:
                    for c in many_commands:
                        self.run_ssh_client_command(ssh_client, c)
                    success = True
                return success

        task = CustomSSHOperator(task_id="test", ssh_hook=self.hook, dag=self.dag, banner_timeout=100)
        se = SSHClientSideEffect(self.hook)
        with unittest.mock.patch.object(task, 'get_ssh_client') as mock_get, unittest.mock.patch.object(
            task, 'run_ssh_client_command'
        ) as mock_run_cmd, unittest.mock.patch('paramiko.client.SSHClient.close') as mock_close:
            mock_get.side_effect = se
            task.execute()
            mock_get.assert_called_once()
            mock_close.assert_called_once()

            ssh_client = se.return_value
            calls = [unittest.mock.call(ssh_client, c) for c in many_commands]
            mock_run_cmd.assert_has_calls(calls)

    def test_fail_with_no_command(self):
        # Test that run_ssh_client_command fails on no command
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            # command="ls",
            dag=self.dag,
        )
        with pytest.raises(AirflowException, match="SSH command not specified. Aborting."):
            task.execute(None)

    def test_command_errored(self):
        # Test that run_ssh_client_command works on invalid commands
        command = "not_a_real_command"
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=command,
            dag=self.dag,
            banner_timeout=100,
        )
        with pytest.raises(AirflowException, match=f"error running cmd: {command}, error: .*"):
            task.execute(None)
