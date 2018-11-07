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

import os
import unittest
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import State
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2016, 1, 2, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


class BashOperatorTestCase(unittest.TestCase):
    def test_echo_env_variables(self):
        """
        Test that env variables are exported correctly to the
        task bash environment.
        """
        now = datetime.utcnow()
        now = now.replace(tzinfo=timezone.utc)

        self.dag = DAG(
            dag_id='bash_op_test', default_args={
                'owner': 'airflow',
                'retries': 100,
                'start_date': DEFAULT_DATE
            },
            schedule_interval='@daily',
            dagrun_timeout=timedelta(minutes=60))

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=now,
            state=State.RUNNING,
            external_trigger=False,
        )

        import tempfile
        with tempfile.NamedTemporaryFile() as f:
            fname = f.name
            t = BashOperator(
                task_id='echo_env_vars',
                dag=self.dag,
                bash_command='echo $AIRFLOW_HOME>> {0};'
                             'echo $PYTHONPATH>> {0};'
                             'echo $AIRFLOW_CTX_DAG_ID >> {0};'
                             'echo $AIRFLOW_CTX_TASK_ID>> {0};'
                             'echo $AIRFLOW_CTX_EXECUTION_DATE>> {0};'
                             'echo $AIRFLOW_CTX_DAG_RUN_ID>> {0};'.format(fname)
            )

            original_AIRFLOW_HOME = os.environ['AIRFLOW_HOME']

            os.environ['AIRFLOW_HOME'] = 'MY_PATH_TO_AIRFLOW_HOME'
            t.run(DEFAULT_DATE, DEFAULT_DATE,
                  ignore_first_depends_on_past=True, ignore_ti_state=True)

            with open(fname, 'r') as fr:
                output = ''.join(fr.readlines())
                self.assertIn('MY_PATH_TO_AIRFLOW_HOME', output)
                # exported in run_unit_tests.sh as part of PYTHONPATH
                self.assertIn('tests/test_utils', output)
                self.assertIn('bash_op_test', output)
                self.assertIn('echo_env_vars', output)
                self.assertIn(DEFAULT_DATE.isoformat(), output)
                self.assertIn('manual__' + DEFAULT_DATE.isoformat(), output)

            os.environ['AIRFLOW_HOME'] = original_AIRFLOW_HOME
