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
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskInstance
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils import timezone
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars

TEST_DAG_ID = 'unit_tests_ssh_test_op'
TEST_CONN_ID = "conn_id_for_testing"
TIMEOUT = 5
DEFAULT_DATE = datetime(2017, 1, 1)
COMMAND = "echo -n airflow"
COMMAND_WITH_SUDO = "sudo " + COMMAND


class TestSSHOperator(unittest.TestCase):
    def setUp(self):
        from airflow.providers.ssh.hooks.ssh import SSHHook

        hook = SSHHook(ssh_conn_id='ssh_default')
        hook.no_host_key_check = True
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
        }
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'
        self.hook = hook
        self.dag = dag

    def test_hook_created_correctly(self):
        timeout = 20
        ssh_id = "ssh_default"
        task = SSHOperator(
            task_id="test", command=COMMAND, dag=self.dag, timeout=timeout, ssh_conn_id="ssh_default"
        )
        assert task is not None

        task.execute(None)

        assert timeout == task.ssh_hook.timeout
        assert ssh_id == task.ssh_hook.ssh_conn_id

    @conf_vars({('core', 'enable_xcom_pickling'): 'False'})
    def test_json_command_execution(self):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=COMMAND,
            do_xcom_push=True,
            dag=self.dag,
        )

        assert task is not None

        ti = TaskInstance(task=task, execution_date=timezone.utcnow())
        ti.run()
        assert ti.duration is not None
        assert ti.xcom_pull(task_ids='test', key='return_value') == b64encode(b'airflow').decode('utf-8')

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_pickle_command_execution(self):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=COMMAND,
            do_xcom_push=True,
            dag=self.dag,
        )

        assert task is not None

        ti = TaskInstance(task=task, execution_date=timezone.utcnow())
        ti.run()
        assert ti.duration is not None
        assert ti.xcom_pull(task_ids='test', key='return_value') == b'airflow'

    def test_command_execution_with_env(self):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=COMMAND,
            do_xcom_push=True,
            dag=self.dag,
            environment={'TEST': 'value'},
        )

        assert task is not None

        with conf_vars({('core', 'enable_xcom_pickling'): 'True'}):
            ti = TaskInstance(task=task, execution_date=timezone.utcnow())
            ti.run()
            assert ti.duration is not None
            assert ti.xcom_pull(task_ids='test', key='return_value') == b'airflow'

    def test_no_output_command(self):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command="sleep 1",
            do_xcom_push=True,
            dag=self.dag,
        )

        assert task is not None

        with conf_vars({('core', 'enable_xcom_pickling'): 'True'}):
            ti = TaskInstance(task=task, execution_date=timezone.utcnow())
            ti.run()
            assert ti.duration is not None
            assert ti.xcom_pull(task_ids='test', key='return_value') == b''

    @unittest.mock.patch('os.environ', {'AIRFLOW_CONN_' + TEST_CONN_ID.upper(): "ssh://test_id@localhost"})
    def test_arg_checking(self):
        # Exception should be raised if neither ssh_hook nor ssh_conn_id is provided
        with pytest.raises(AirflowException, match="Cannot operate without ssh_hook or ssh_conn_id."):
            task_0 = SSHOperator(task_id="test", command=COMMAND, timeout=TIMEOUT, dag=self.dag)
            task_0.execute(None)

        # if ssh_hook is invalid/not provided, use ssh_conn_id to create SSHHook
        task_1 = SSHOperator(
            task_id="test_1",
            ssh_hook="string_rather_than_SSHHook",  # invalid ssh_hook
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
            timeout=TIMEOUT,
            dag=self.dag,
        )
        try:
            task_1.execute(None)
        except Exception:  # pylint: disable=broad-except
            pass
        assert task_1.ssh_hook.ssh_conn_id == TEST_CONN_ID

        task_2 = SSHOperator(
            task_id="test_2",
            ssh_conn_id=TEST_CONN_ID,  # no ssh_hook provided
            command=COMMAND,
            timeout=TIMEOUT,
            dag=self.dag,
        )
        try:
            task_2.execute(None)
        except Exception:  # pylint: disable=broad-except
            pass
        assert task_2.ssh_hook.ssh_conn_id == TEST_CONN_ID

        # if both valid ssh_hook and ssh_conn_id are provided, ignore ssh_conn_id
        task_3 = SSHOperator(
            task_id="test_3",
            ssh_hook=self.hook,
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
            timeout=TIMEOUT,
            dag=self.dag,
        )
        try:
            task_3.execute(None)
        except Exception:  # pylint: disable=broad-except
            pass
        assert task_3.ssh_hook.ssh_conn_id == self.hook.ssh_conn_id

    @parameterized.expand(
        [
            (COMMAND, False, False),
            (COMMAND, True, True),
            (COMMAND_WITH_SUDO, False, True),
            (COMMAND_WITH_SUDO, True, True),
            (None, True, True),
        ]
    )
    def test_get_pyt_set_correctly(self, command, get_pty_in, get_pty_out):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=command,
            timeout=TIMEOUT,
            get_pty=get_pty_in,
            dag=self.dag,
        )
        try:
            task.execute(None)
        except Exception:  # pylint: disable=broad-except
            pass
        assert task.get_pty == get_pty_out
