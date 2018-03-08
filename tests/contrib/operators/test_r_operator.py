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

from __future__ import print_function, unicode_literals

import os
import unittest

from airflow import configuration, DAG
from airflow.contrib.operators.r_operator import ROperator
from airflow.models import TaskInstance
from airflow.utils import timezone


DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class ROperatorTest(unittest.TestCase):
    """Test the ROperator"""

    def setUp(self):
        super(ROperatorTest, self).setUp()
        configuration.load_test_config()
        self.dag = DAG(
            'test_roperator_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE
            },
            schedule_interval='@once'
        )

        self.xcom_test_str = 'Hello Airflow'
        self.task_xcom = ROperator(
            task_id='test_r_xcom',
            r_command='cat("Ignored Line\n{}")'.format(self.xcom_test_str),
            xcom_push=True,
            dag=self.dag
        )

    def test_invalid_rscript_bin(self):
        """Fail if invalid rscript_bin supplied"""

        try:
            expected_error = FileNotFoundError
        except NameError:
            # py2
            expected_error = OSError

        task = ROperator(
            task_id='test_r_bad_rscript',
            r_command='print(Sys.Date())',
            rscript_bin='somebadrscript',
            dag=self.dag
        )

        self.assertIsNotNone(task)

        ti = TaskInstance(task=task, execution_date=timezone.utcnow())

        with self.assertRaises(expected_error):
            ti.run()

    def test_xcom_output(self):
        """Test whether Xcom output is produced using last line"""

        self.task_xcom.xcom_push = True

        ti = TaskInstance(
            task=self.task_xcom,
            execution_date=timezone.utcnow()
        )

        ti.run()
        self.assertIsNotNone(ti.duration)

        self.assertEqual(
            ti.xcom_pull(task_ids=self.task_xcom.task_id, key='return_value'),
            self.xcom_test_str
        )

    def test_xcom_none(self):
        """Test whether no Xcom output is produced when push=False"""

        self.task_xcom.xcom_push = False

        ti = TaskInstance(
            task=self.task_xcom,
            execution_date=timezone.utcnow(),
        )

        ti.run()
        self.assertIsNotNone(ti.duration)
        self.assertIsNone(ti.xcom_pull(task_ids=self.task_xcom.task_id))

    def test_env_vars(self):
        """Test whether environment is passed properly"""

        test_var = 'TEST_VALUE_X'
        test_str = 'Hello Airflow'

        task = ROperator(
            task_id='test_env_vars',
            r_command='cat(Sys.getenv("{}"))'.format(test_var),
            env={test_var: test_str, "PATH": os.environ['PATH']},
            xcom_push=True,
            dag=self.dag
        )

        ti = TaskInstance(task=task, execution_date=timezone.utcnow())

        ti.run()
        self.assertIsNotNone(ti.duration)

        self.assertEqual(
            ti.xcom_pull(task_ids=task.task_id, key='return_value'),
            test_str
        )

    def test_command_template(self):
        """Test whether templating works properly with r_command"""

        task = ROperator(
            task_id='test_cmd_template',
            r_command='cat("{{ ds }}")',
            dag=self.dag
        )

        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.render_templates()

        self.assertEqual(
            ti.task.r_command,
            'cat("{}")'.format(DEFAULT_DATE.date().isoformat())
        )

    def test_env_templates(self):
        """Test whether templating works properly with env vars"""

        test_var = 'TEST_CURR_DATE'

        task = ROperator(
            task_id='test_env_template',
            r_command='cat(Sys.getenv("{}"))'.format(test_var),
            env={test_var: "{{ ds }}"},
            xcom_push=True,
            dag=self.dag
        )

        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.run()

        self.assertEqual(
            ti.xcom_pull(task_ids=task.task_id, key='return_value'),
            DEFAULT_DATE.date().isoformat()
        )


if __name__ == '__main__':
    unittest.main()
