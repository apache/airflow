# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from base64 import b64encode
from datetime import datetime

from airflow import configuration
from airflow import models
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import DAG, TaskInstance
from airflow.settings import Session

TEST_DAG_ID = 'unit_tests'
DEFAULT_DATE = datetime(2017, 1, 1)


def reset(dag_id=TEST_DAG_ID):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()

reset()


class SSHOperatorTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
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

    def test_json_command_execution(self):
        configuration.set("core", "enable_xcom_pickling", "False")
        task = SSHOperator(
                task_id="test",
                ssh_hook=self.hook,
                command="echo -n airflow",
                do_xcom_push=True,
                dag=self.dag,
        )

        self.assertIsNotNone(task)

        ti = TaskInstance(
                task=task, execution_date=datetime.now())
        ti.run()
        self.assertIsNotNone(ti.duration)
        self.assertEqual(ti.xcom_pull(task_ids='test', key='return_value'),
                         b64encode(b'airflow').decode('utf-8'))

    def test_pickle_command_execution(self):
        configuration.set("core", "enable_xcom_pickling", "True")
        task = SSHOperator(
                task_id="test",
                ssh_hook=self.hook,
                command="echo -n airflow",
                do_xcom_push=True,
                dag=self.dag,
        )

        self.assertIsNotNone(task)

        ti = TaskInstance(
                task=task, execution_date=datetime.now())
        ti.run()
        self.assertIsNotNone(ti.duration)
        self.assertEqual(ti.xcom_pull(task_ids='test', key='return_value'), b'airflow')

    def test_command_execution_with_env(self):
        configuration.set("core", "enable_xcom_pickling", "True")
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command="echo -n airflow",
            do_xcom_push=True,
            dag=self.dag,
        )

        self.assertIsNotNone(task)

        ti = TaskInstance(
            task=task, execution_date=datetime.now())
        ti.run()
        self.assertIsNotNone(ti.duration)
        self.assertEqual(ti.xcom_pull(task_ids='test', key='return_value'), b'airflow')

    def test_no_output_command(self):
        configuration.set("core", "enable_xcom_pickling", "True")
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command="sleep 1",
            do_xcom_push=True,
            dag=self.dag,
        )

        self.assertIsNotNone(task)

        ti = TaskInstance(
            task=task, execution_date=datetime.now())
        ti.run()
        self.assertIsNotNone(ti.duration)
        self.assertEqual(ti.xcom_pull(task_ids='test', key='return_value'), b'')

if __name__ == '__main__':
    unittest.main()
