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
import signal
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile, TemporaryDirectory
from time import sleep
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException, AirflowSkipException, AirflowTaskTimeout
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2016, 1, 2, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


class TestBashOperator:
    @parameterized.expand(
        [
            (False, None, 'MY_PATH_TO_AIRFLOW_HOME'),
            (True, {'AIRFLOW_HOME': 'OVERRIDDEN_AIRFLOW_HOME'}, 'OVERRIDDEN_AIRFLOW_HOME'),
        ]
    )
    def test_echo_env_variables(self, append_env, user_defined_env, expected_airflow_home):
        """
        Test that env variables are exported correctly to the task bash environment.
        """
        utc_now = datetime.utcnow().replace(tzinfo=timezone.utc)
        expected = (
            f"{expected_airflow_home}\n"
            "AWESOME_PYTHONPATH\n"
            "bash_op_test\n"
            "echo_env_vars\n"
            f"{utc_now.isoformat()}\n"
            f"manual__{utc_now.isoformat()}\n"
        )

        dag = DAG(
            dag_id='bash_op_test',
            default_args={'owner': 'airflow', 'retries': 100, 'start_date': DEFAULT_DATE},
            schedule_interval='@daily',
            dagrun_timeout=timedelta(minutes=60),
        )

        dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=utc_now,
            start_date=utc_now,
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
                'echo $AIRFLOW_CTX_DAG_RUN_ID>> {0};'.format(tmp_file.name),
                append_env=append_env,
                env=user_defined_env,
            )

            with mock.patch.dict(
                'os.environ', {'AIRFLOW_HOME': 'MY_PATH_TO_AIRFLOW_HOME', 'PYTHONPATH': 'AWESOME_PYTHONPATH'}
            ):
                task.run(utc_now, utc_now, ignore_first_depends_on_past=True, ignore_ti_state=True)

            with open(tmp_file.name) as file:
                output = ''.join(file.readlines())
                assert expected == output

    @parameterized.expand(
        [
            ('test-val', 'test-val'),
            ('test-val\ntest-val\n', ''),
            ('test-val\ntest-val', 'test-val'),
            ('', ''),
        ]
    )
    def test_return_value(self, val, expected):
        op = BashOperator(task_id='abc', bash_command=f'set -e; echo "{val}";')
        line = op.execute({})
        assert line == expected

    def test_raise_exception_on_non_zero_exit_code(self):
        bash_operator = BashOperator(bash_command='exit 42', task_id='test_return_value', dag=None)
        with pytest.raises(
            AirflowException, match="Bash command failed\\. The command returned a non-zero exit code 42\\."
        ):
            bash_operator.execute(context={})

    def test_task_retries(self):
        bash_operator = BashOperator(
            bash_command='echo "stdout"', task_id='test_task_retries', retries=2, dag=None
        )

        assert bash_operator.retries == 2

    def test_default_retries(self):
        bash_operator = BashOperator(bash_command='echo "stdout"', task_id='test_default_retries', dag=None)

        assert bash_operator.retries == 0

    def test_command_not_found(self):
        with pytest.raises(
            AirflowException, match="Bash command failed\\. The command returned a non-zero exit code 127\\."
        ):
            BashOperator(task_id='abc', bash_command='set -e; something-that-isnt-on-path').execute({})

    def test_unset_cwd(self):
        val = "xxxx"
        op = BashOperator(task_id='abc', bash_command=f'set -e; echo "{val}";')
        line = op.execute({})
        assert line == val

    def test_cwd_does_not_exist(self):
        test_cmd = 'set -e; echo "xxxx" |tee outputs.txt'
        with TemporaryDirectory(prefix='test_command_with_cwd') as tmp_dir:
            # Get a nonexistent temporary directory to do the test
            pass
        # There should be no exceptions when creating the operator even the `cwd` doesn't exist
        bash_operator = BashOperator(task_id='abc', bash_command=test_cmd, cwd=tmp_dir)
        with pytest.raises(AirflowException, match=f"Can not find the cwd: {tmp_dir}"):
            bash_operator.execute({})

    def test_cwd_is_file(self):
        test_cmd = 'set -e; echo "xxxx" |tee outputs.txt'
        with NamedTemporaryFile(suffix="var.env") as tmp_file:
            # Test if the cwd is a file_path
            with pytest.raises(AirflowException, match=f"The cwd {tmp_file.name} must be a directory"):
                BashOperator(task_id='abc', bash_command=test_cmd, cwd=tmp_file.name).execute({})

    def test_valid_cwd(self):
        test_cmd = 'set -e; echo "xxxx" |tee outputs.txt'
        with TemporaryDirectory(prefix='test_command_with_cwd') as test_cwd_folder:
            # Test everything went alright
            result = BashOperator(task_id='abc', bash_command=test_cmd, cwd=test_cwd_folder).execute({})
            assert result == "xxxx"
            with open(f'{test_cwd_folder}/outputs.txt') as tmp_file:
                assert tmp_file.read().splitlines()[0] == "xxxx"

    @parameterized.expand(
        [
            (None, 99, AirflowSkipException),
            ({'skip_exit_code': 100}, 100, AirflowSkipException),
            ({'skip_exit_code': 100}, 101, AirflowException),
            ({'skip_exit_code': None}, 99, AirflowException),
        ]
    )
    def test_skip(self, extra_kwargs, actual_exit_code, expected_exc):
        kwargs = dict(task_id='abc', bash_command=f'set -e; echo "hello world"; exit {actual_exit_code};')
        if extra_kwargs:
            kwargs.update(**extra_kwargs)
        with pytest.raises(expected_exc):
            BashOperator(**kwargs).execute({})

    def test_bash_operator_multi_byte_output(self):
        op = BashOperator(
            task_id='test_multi_byte_bash_operator',
            bash_command="echo \u2600",
            output_encoding='utf-8',
        )
        op.execute(context={})

    def test_bash_operator_kill(self, dag_maker):
        import psutil

        sleep_time = "100%d" % os.getpid()
        with dag_maker():
            op = BashOperator(
                task_id='test_bash_operator_kill',
                execution_timeout=timedelta(microseconds=25),
                bash_command=f"/bin/bash -c 'sleep {sleep_time}'",
            )
        with pytest.raises(AirflowTaskTimeout):
            op.run()
        sleep(2)
        for proc in psutil.process_iter():
            if proc.cmdline() == ['sleep', sleep_time]:
                os.kill(proc.pid, signal.SIGTERM)
                assert False, "BashOperator's subprocess still running after stopping on timeout!"
                break
