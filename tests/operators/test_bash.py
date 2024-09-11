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
from __future__ import annotations

import json
import os
import signal
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, AirflowSkipException, AirflowTaskTimeout
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2016, 1, 2, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


@pytest.fixture
def context():
    return {"ti": mock.Mock()}


class TestBashOperator:
    def test_bash_operator_init(self):
        """Test the construction of the operator with its defaults and initially-derived attrs."""
        op = BashOperator(task_id="bash_op", bash_command="echo")

        assert op.bash_command == "echo"
        assert op.env is None
        assert op.append_env is False
        assert op.output_encoding == "utf-8"
        assert op.skip_on_exit_code == [99]
        assert op.cwd is None
        assert op._init_bash_command_not_set is False

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "append_env,user_defined_env,expected_airflow_home",
        [
            (False, None, "MY_PATH_TO_AIRFLOW_HOME"),
            (True, {"AIRFLOW_HOME": "OVERRIDDEN_AIRFLOW_HOME"}, "OVERRIDDEN_AIRFLOW_HOME"),
        ],
    )
    def test_echo_env_variables(
        self, append_env, user_defined_env, expected_airflow_home, dag_maker, tmp_path
    ):
        """
        Test that env variables are exported correctly to the task bash environment.
        """
        utc_now = datetime.now(tz=timezone.utc)
        expected = (
            f"{expected_airflow_home}\n"
            "AWESOME_PYTHONPATH\n"
            "bash_op_test\n"
            "echo_env_vars\n"
            f"{utc_now.isoformat()}\n"
            f"manual__{utc_now.isoformat()}\n"
        )

        with dag_maker(
            "bash_op_test",
            default_args={"owner": "airflow", "retries": 100, "start_date": DEFAULT_DATE},
            schedule="@daily",
            dagrun_timeout=timedelta(minutes=60),
            serialized=True,
        ):
            tmp_file = tmp_path / "testfile"
            task = BashOperator(
                task_id="echo_env_vars",
                bash_command=f"echo $AIRFLOW_HOME>> {tmp_file};"
                f"echo $PYTHONPATH>> {tmp_file};"
                f"echo $AIRFLOW_CTX_DAG_ID >> {tmp_file};"
                f"echo $AIRFLOW_CTX_TASK_ID>> {tmp_file};"
                f"echo $AIRFLOW_CTX_EXECUTION_DATE>> {tmp_file};"
                f"echo $AIRFLOW_CTX_DAG_RUN_ID>> {tmp_file};",
                append_env=append_env,
                env=user_defined_env,
            )

        execution_date = utc_now
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=execution_date,
            start_date=utc_now,
            state=State.RUNNING,
            external_trigger=False,
            data_interval=(execution_date, execution_date),
            **triggered_by_kwargs,
        )

        with mock.patch.dict(
            "os.environ", {"AIRFLOW_HOME": "MY_PATH_TO_AIRFLOW_HOME", "PYTHONPATH": "AWESOME_PYTHONPATH"}
        ):
            task.run(utc_now, utc_now, ignore_first_depends_on_past=True, ignore_ti_state=True)

        assert expected == tmp_file.read_text()

    @pytest.mark.parametrize(
        "val,expected",
        [
            ("test-val", "test-val"),
            ("test-val\ntest-val\n", ""),
            ("test-val\ntest-val", "test-val"),
            ("", ""),
        ],
    )
    def test_return_value(self, val, expected, context):
        op = BashOperator(task_id="abc", bash_command=f'set -e; echo "{val}";')
        line = op.execute(context)
        assert line == expected

    def test_raise_exception_on_non_zero_exit_code(self, context):
        bash_operator = BashOperator(bash_command="exit 42", task_id="test_return_value", dag=None)
        with pytest.raises(
            AirflowException, match="Bash command failed\\. The command returned a non-zero exit code 42\\."
        ):
            bash_operator.execute(context)

    def test_task_retries(self):
        bash_operator = BashOperator(
            bash_command='echo "stdout"', task_id="test_task_retries", retries=2, dag=None
        )

        assert bash_operator.retries == 2

    def test_default_retries(self):
        bash_operator = BashOperator(bash_command='echo "stdout"', task_id="test_default_retries", dag=None)

        assert bash_operator.retries == 0

    def test_command_not_found(self, context):
        with pytest.raises(
            AirflowException, match="Bash command failed\\. The command returned a non-zero exit code 127\\."
        ):
            BashOperator(task_id="abc", bash_command="set -e; something-that-isnt-on-path").execute(context)

    def test_unset_cwd(self, context):
        val = "xxxx"
        op = BashOperator(task_id="abc", bash_command=f'set -e; echo "{val}";')
        line = op.execute(context)
        assert line == val

    def test_cwd_does_not_exist(self, context, tmp_path):
        test_cmd = 'set -e; echo "xxxx" |tee outputs.txt'
        test_cwd_folder = os.fspath(tmp_path / "test_command_with_cwd")
        # There should be no exceptions when creating the operator even the `cwd` doesn't exist
        bash_operator = BashOperator(task_id="abc", bash_command=test_cmd, cwd=os.fspath(test_cwd_folder))
        with pytest.raises(AirflowException, match=f"Can not find the cwd: {test_cwd_folder}"):
            bash_operator.execute(context)

    def test_cwd_is_file(self, tmp_path):
        test_cmd = 'set -e; echo "xxxx" |tee outputs.txt'
        tmp_file = tmp_path / "testfile.var.env"
        tmp_file.touch()
        # Test if the cwd is a file_path
        with pytest.raises(AirflowException, match=f"The cwd {tmp_file} must be a directory"):
            BashOperator(task_id="abc", bash_command=test_cmd, cwd=os.fspath(tmp_file)).execute({})

    def test_valid_cwd(self, context, tmp_path):
        test_cmd = 'set -e; echo "xxxx" |tee outputs.txt'
        test_cwd_path = tmp_path / "test_command_with_cwd"
        test_cwd_path.mkdir()
        # Test everything went alright
        result = BashOperator(task_id="abc", bash_command=test_cmd, cwd=os.fspath(test_cwd_path)).execute(
            context
        )
        assert result == "xxxx"
        assert (test_cwd_path / "outputs.txt").read_text().splitlines()[0] == "xxxx"

    @pytest.mark.parametrize(
        "extra_kwargs,actual_exit_code,expected_exc",
        [
            ({}, 0, None),
            ({}, 100, AirflowException),
            ({}, 99, AirflowSkipException),
            ({"skip_on_exit_code": None}, 0, None),
            ({"skip_on_exit_code": None}, 100, AirflowException),
            ({"skip_on_exit_code": None}, 99, AirflowException),
            ({"skip_on_exit_code": 100}, 0, None),
            ({"skip_on_exit_code": 100}, 100, AirflowSkipException),
            ({"skip_on_exit_code": 100}, 99, AirflowException),
            ({"skip_on_exit_code": 0}, 0, AirflowSkipException),
            ({"skip_on_exit_code": [100]}, 0, None),
            ({"skip_on_exit_code": [100]}, 100, AirflowSkipException),
            ({"skip_on_exit_code": [100]}, 99, AirflowException),
            ({"skip_on_exit_code": [100, 102]}, 99, AirflowException),
            ({"skip_on_exit_code": (100,)}, 0, None),
            ({"skip_on_exit_code": (100,)}, 100, AirflowSkipException),
            ({"skip_on_exit_code": (100,)}, 99, AirflowException),
        ],
    )
    def test_skip(self, extra_kwargs, actual_exit_code, expected_exc, context):
        kwargs = dict(task_id="abc", bash_command=f'set -e; echo "hello world"; exit {actual_exit_code};')
        if extra_kwargs:
            kwargs.update(**extra_kwargs)
        if expected_exc is None:
            BashOperator(**kwargs).execute(context)
        else:
            with pytest.raises(expected_exc):
                BashOperator(**kwargs).execute(context)

    def test_bash_operator_multi_byte_output(self, context):
        op = BashOperator(
            task_id="test_multi_byte_bash_operator",
            bash_command="echo \u2600",
            output_encoding="utf-8",
        )
        op.execute(context)

    def test_bash_operator_output_processor(self, context):
        json_string = '{"AAD_BASIC": "Azure Active Directory Basic"}'
        op = BashOperator(
            task_id="test_bash_operator_output_processor",
            bash_command=f"echo '{json_string}'",
            output_processor=lambda output: json.loads(output),
        )
        result = op.execute(context)
        assert result == json.loads(json_string)

    @pytest.mark.db_test
    def test_bash_operator_kill(self, dag_maker):
        import psutil

        sleep_time = f"100{os.getpid()}"
        with dag_maker(serialized=True):
            op = BashOperator(
                task_id="test_bash_operator_kill",
                execution_timeout=timedelta(microseconds=25),
                bash_command=f"/bin/bash -c 'sleep {sleep_time}'",
            )
        dag_maker.create_dagrun()
        with pytest.raises(AirflowTaskTimeout):
            op.run()
        sleep(2)
        for proc in psutil.process_iter():
            if proc.cmdline() == ["sleep", sleep_time]:
                os.kill(proc.pid, signal.SIGTERM)
                pytest.fail("BashOperator's subprocess still running after stopping on timeout!")

    @pytest.mark.db_test
    def test_templated_fields(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            BashOperator,
            # Templated fields
            bash_command='echo "{{ dag_run.dag_id }}"',
            env={"FOO": "{{ ds }}"},
            cwd="{{ dag_run.dag.folder }}",
            # Other parameters
            dag_id="test_templated_fields_dag",
            task_id="test_templated_fields_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: BashOperator = ti.task
        assert task.bash_command == 'echo "test_templated_fields_dag"'
        assert task.env == {"FOO": "2024-02-01"}
        assert task.cwd == Path(__file__).absolute().parent.as_posix()
