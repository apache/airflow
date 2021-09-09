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


class TestSSHOperator:
    def setup_method(self):
        from airflow.providers.ssh.hooks.ssh import SSHHook

        hook = SSHHook(ssh_conn_id='ssh_default')
        hook.no_host_key_check = True
        self.hook = hook

    def test_hook_created_correctly_with_timeout(self):
        timeout = 20
        ssh_id = "ssh_default"
        with DAG('unit_tests_ssh_test_op_arg_checking', default_args={'start_date': DEFAULT_DATE}):
            task = SSHOperator(task_id="test", command=COMMAND, timeout=timeout, ssh_conn_id="ssh_default")
        task.execute(None)
        assert timeout == task.ssh_hook.conn_timeout
        assert ssh_id == task.ssh_hook.ssh_conn_id

    def test_hook_created_correctly(self):
        conn_timeout = 20
        cmd_timeout = 45
        ssh_id = 'ssh_default'
        with DAG('unit_tests_ssh_test_op_arg_checking', default_args={'start_date': DEFAULT_DATE}):
            task = SSHOperator(
                task_id="test",
                command=COMMAND,
                conn_timeout=conn_timeout,
                cmd_timeout=cmd_timeout,
                ssh_conn_id="ssh_default",
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
        )
        ti.run()
        assert ti.duration is not None
        assert ti.xcom_pull(task_ids='test', key='return_value') == b''

    @unittest.mock.patch('os.environ', {'AIRFLOW_CONN_' + TEST_CONN_ID.upper(): "ssh://test_id@localhost"})
    def test_arg_checking(self):
        dag = DAG('unit_tests_ssh_test_op_arg_checking', default_args={'start_date': DEFAULT_DATE})

        # Exception should be raised if neither ssh_hook nor ssh_conn_id is provided.
        task_0 = SSHOperator(task_id="test", command=COMMAND, timeout=TIMEOUT, dag=dag)
        with pytest.raises(AirflowException, match="Cannot operate without ssh_hook or ssh_conn_id."):
            task_0.execute(None)

        # If ssh_hook is invalid/not provided, use ssh_conn_id to create SSHHook.
        task_1 = SSHOperator(
            task_id="test_1",
            ssh_hook="string_rather_than_SSHHook",  # Invalid ssh_hook.
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
            timeout=TIMEOUT,
            dag=dag,
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
            dag=dag,
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
            dag=dag,
        )
        task_3.execute(None)
        assert task_3.ssh_hook.ssh_conn_id == self.hook.ssh_conn_id

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
        dag = DAG('unit_tests_ssh_test_op_arg_checking', default_args={'start_date': DEFAULT_DATE})
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=command,
            conn_timeout=TIMEOUT,
            cmd_timeout=TIMEOUT,
            get_pty=get_pty_in,
            dag=dag,
        )
        if command is None:
            with pytest.raises(AirflowException) as ctx:
                task.execute(None)
            assert str(ctx.value) == "SSH operator error: SSH command not specified. Aborting."
        else:
            task.execute(None)
        assert task.get_pty == get_pty_out
