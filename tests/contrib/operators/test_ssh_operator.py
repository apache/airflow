# -*- coding: utf-8 -*-
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

from base64 import b64encode
from unittest import TestCase, main

import six


from parameterized import parameterized
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import DAG, TaskInstance
from airflow.utils import timezone
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars

TEST_CONN_ID = "conn_id_for_testing"
TIMEOUT = 5
TEST_DAG_ID = 'unit_tests_ssh_test_op'
DEFAULT_DATE = datetime(2017, 1, 1)
COMMAND = "echo -n airflow"
COMMAND_WITH_SUDO = "sudo " + COMMAND


class SSHOperatorTest(TestCase):
    def setUp(self):
        from airflow.contrib.hooks.ssh_hook import SSHHook
        hook = SSHHook(ssh_conn_id='ssh_default')
        hook.no_host_key_check = True
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
            'provide_context': True
        }
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'
        self.hook = hook
        self.dag = dag

    def test_hook_created_correctly(self):
        TIMEOUT = 20
        SSH_ID = "ssh_default"
        task = SSHOperator(
            task_id="test",
            command=COMMAND,
            dag=self.dag,
            timeout=TIMEOUT,
            ssh_conn_id="ssh_default"
        )
        self.assertIsNotNone(task)

        task.execute(None)

        self.assertEqual(TIMEOUT, task.ssh_hook.timeout)
        self.assertEqual(SSH_ID, task.ssh_hook.ssh_conn_id)

    @conf_vars({('core', 'enable_xcom_pickling'): 'False'})
    def test_json_command_execution(self):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=COMMAND,
            do_xcom_push=True,
            dag=self.dag,
        )

        self.assertIsNotNone(task)

        ti = TaskInstance(
            task=task, execution_date=timezone.utcnow())
        ti.run()
        self.assertIsNotNone(ti.duration)
        self.assertEqual(ti.xcom_pull(task_ids='test', key='return_value'),
                         b64encode(b'airflow').decode('utf-8'))

    @conf_vars({('core', 'enable_xcom_pickling'): 'True'})
    def test_pickle_command_execution(self):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=COMMAND,
            do_xcom_push=True,
            dag=self.dag,
        )

        self.assertIsNotNone(task)

        ti = TaskInstance(
            task=task, execution_date=timezone.utcnow())
        ti.run()
        self.assertIsNotNone(ti.duration)
        self.assertEqual(ti.xcom_pull(task_ids='test', key='return_value'), b'airflow')

    def test_command_execution_with_env(self):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=COMMAND,
            do_xcom_push=True,
            dag=self.dag,
            environment={'TEST': 'value'}
        )

        self.assertIsNotNone(task)

        with conf_vars({('core', 'enable_xcom_pickling'): 'True'}):
            ti = TaskInstance(
                task=task, execution_date=timezone.utcnow())
            ti.run()
            self.assertIsNotNone(ti.duration)
            self.assertEqual(ti.xcom_pull(task_ids='test', key='return_value'), b'airflow')

    def test_no_output_command(self):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command="sleep 1",
            do_xcom_push=True,
            dag=self.dag,
        )

        self.assertIsNotNone(task)

        with conf_vars({('core', 'enable_xcom_pickling'): 'True'}):
            ti = TaskInstance(
                task=task, execution_date=timezone.utcnow())
            ti.run()
            self.assertIsNotNone(ti.duration)
            self.assertEqual(ti.xcom_pull(task_ids='test', key='return_value'), b'')

    def test_arg_checking(self):
        import os
        from airflow.exceptions import AirflowException
        conn_id = "conn_id_for_testing"
        TIMEOUT = 5
        os.environ['AIRFLOW_CONN_' + conn_id.upper()] = "ssh://test_id@localhost"

        # Exception should be raised if neither ssh_hook nor ssh_conn_id is provided
        if six.PY2:
            self.assertRaisesRegex = self.assertRaisesRegexp
        with self.assertRaisesRegex(AirflowException,
                                    "Cannot operate without ssh_hook or ssh_conn_id."):
            task_0 = SSHOperator(task_id="test", command=COMMAND,
                                 timeout=TIMEOUT, dag=self.dag)
            task_0.execute(None)

        # if ssh_hook is invalid/not provided, use ssh_conn_id to create SSHHook
        task_1 = SSHOperator(
            task_id="test_1",
            ssh_hook="string_rather_than_SSHHook",  # invalid ssh_hook
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
            timeout=TIMEOUT,
            dag=self.dag
        )
        try:
            task_1.execute(None)
        except Exception:
            pass
        self.assertEqual(task_1.ssh_hook.ssh_conn_id, conn_id)

        task_2 = SSHOperator(
            task_id="test_2",
            ssh_conn_id=TEST_CONN_ID,  # no ssh_hook provided
            command=COMMAND,
            timeout=TIMEOUT,
            dag=self.dag
        )
        try:
            task_2.execute(None)
        except Exception:
            pass
        self.assertEqual(task_2.ssh_hook.ssh_conn_id, conn_id)

        # if both valid ssh_hook and ssh_conn_id are provided, ignore ssh_conn_id
        task_3 = SSHOperator(
            task_id="test_3",
            ssh_hook=self.hook,
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
            timeout=TIMEOUT,
            dag=self.dag
        )
        try:
            task_3.execute(None)
        except Exception:
            pass
        self.assertEqual(task_3.ssh_hook.ssh_conn_id, self.hook.ssh_conn_id)

    @parameterized.expand([
        (COMMAND, False, False),
        (COMMAND, True, True),
        (COMMAND_WITH_SUDO, False, True),
        (COMMAND_WITH_SUDO, True, True),
    ])
    def test_get_pyt_set_correctly(self, command, get_pty_in, get_pty_out):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=command,
            timeout=TIMEOUT,
            get_pty=get_pty_in,
            dag=self.dag
        )
        try:
            task.execute(None)
        except Exception:
            pass
        self.assertEqual(task.get_pty, get_pty_out)


if __name__ == '__main__':
    main()
