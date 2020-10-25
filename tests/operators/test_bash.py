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

import unittest
from datetime import datetime, timedelta
from subprocess import PIPE, STDOUT
from tempfile import NamedTemporaryFile
from unittest import mock

from airflow.exceptions import AirflowException
from airflow.models import DagRun
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2016, 1, 2, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


class TestBashOperator(unittest.TestCase):

    def test_echo_env_variables(self):
        """
        Test that env variables are exported correctly to the
        task bash environment.
        """
        now = datetime.utcnow()
        now = now.replace(tzinfo=timezone.utc)

        dag = DAG(
            dag_id='bash_op_test', default_args={
                'owner': 'airflow',
                'retries': 100,
                'start_date': DEFAULT_DATE
            },
            schedule_interval='@daily',
            dagrun_timeout=timedelta(minutes=60))

        dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=now,
            state=State.RUNNING,
            external_trigger=False,
        )

        with NamedTemporaryFile() as tmp_file:
            task = BashOperator(
                task_id='echo_env_vars',
                dag=dag,
                bash_command='echo $AIRFLOW_HOME>> {0};'
                             'echo $PYTHONPATH>> {0};'
                             'echo $AIRFLOW_CTX_DAG_ID >> {0};'
                             'echo $AIRFLOW_CTX_TASK_ID>> {0};'
                             'echo $AIRFLOW_CTX_EXECUTION_DATE>> {0};'
                             'echo $AIRFLOW_CTX_DAG_RUN_ID>> {0};'.format(tmp_file.name)
            )

            with mock.patch.dict('os.environ', {
                'AIRFLOW_HOME': 'MY_PATH_TO_AIRFLOW_HOME',
                'PYTHONPATH': 'AWESOME_PYTHONPATH'
            }):
                task.run(DEFAULT_DATE, DEFAULT_DATE,
                         ignore_first_depends_on_past=True, ignore_ti_state=True)

            with open(tmp_file.name, 'r') as file:
                output = ''.join(file.readlines())
                self.assertIn('MY_PATH_TO_AIRFLOW_HOME', output)
                # exported in run-tests as part of PYTHONPATH
                self.assertIn('AWESOME_PYTHONPATH', output)
                self.assertIn('bash_op_test', output)
                self.assertIn('echo_env_vars', output)
                self.assertIn(DEFAULT_DATE.isoformat(), output)
                self.assertIn(DagRun.generate_run_id(DagRunType.MANUAL, DEFAULT_DATE), output)

    def test_return_value(self):
        bash_operator = BashOperator(
            bash_command='echo "stdout"',
            task_id='test_return_value',
            dag=None
        )
        return_value = bash_operator.execute(context={})

        self.assertEqual(return_value, 'stdout')

    def test_raise_exception_on_non_zero_exit_code(self):
        bash_operator = BashOperator(
            bash_command='exit 42',
            task_id='test_return_value',
            dag=None
        )
        with self.assertRaisesRegex(
            AirflowException,
            "Bash command failed\\. The command returned a non-zero exit code\\."
        ):
            bash_operator.execute(context={})

    def test_task_retries(self):
        bash_operator = BashOperator(
            bash_command='echo "stdout"',
            task_id='test_task_retries',
            retries=2,
            dag=None
        )

        self.assertEqual(bash_operator.retries, 2)

    def test_default_retries(self):
        bash_operator = BashOperator(
            bash_command='echo "stdout"',
            task_id='test_default_retries',
            dag=None
        )

        self.assertEqual(bash_operator.retries, 0)

    @mock.patch.dict('os.environ', clear=True)
    @mock.patch("airflow.operators.bash.TemporaryDirectory", **{  # type: ignore
        'return_value.__enter__.return_value': '/tmp/airflowtmpcatcat'
    })
    @mock.patch("airflow.operators.bash.Popen", **{  # type: ignore
        'return_value.stdout.readline.side_effect': [b'BAR', b'BAZ'],
        'return_value.returncode': 0
    })
    def test_should_exec_subprocess(self, mock_popen, mock_temporary_directory):
        bash_operator = BashOperator(
            bash_command='echo "stdout"',
            task_id='test_return_value',
            dag=None
        )
        bash_operator.execute({})

        mock_popen.assert_called_once_with(
            ['bash', '-c', 'echo "stdout"'],
            cwd='/tmp/airflowtmpcatcat',
            env={},
            preexec_fn=mock.ANY,
            stderr=STDOUT,
            stdout=PIPE
        )
