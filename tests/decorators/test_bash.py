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

import os
import stat
import warnings
from contextlib import nullcontext as no_raise
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow.decorators import task
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.utils import timezone
from airflow.utils.types import NOTSET

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_rendered_ti_fields

if TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.operators.bash import BashOperator

DEFAULT_DATE = timezone.datetime(2023, 1, 1)


@pytest.mark.db_test
class TestBashDecorator:
    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        self.dag_maker = dag_maker

        with dag_maker(dag_id="bash_deco_dag") as dag:
            ...

        self.dag = dag

    def teardown_method(self):
        clear_db_runs()
        clear_db_dags()
        clear_rendered_ti_fields()

    def execute_task(self, task):
        session = self.dag_maker.session
        dag_run = self.dag_maker.create_dagrun(
            run_id=f"bash_deco_test_{DEFAULT_DATE.date()}", session=session
        )
        ti = dag_run.get_task_instance(task.operator.task_id, session=session)
        return_val = task.operator.execute(context={"ti": ti})

        return ti, return_val

    @staticmethod
    def validate_bash_command_rtif(ti, expected_command):
        assert RenderedTaskInstanceFields.get_templated_fields(ti)["bash_command"] == expected_command

    def test_bash_decorator_init(self):
        """Test the initialization of the @task.bash decorator."""

        with self.dag:

            @task.bash
            def bash(): ...

            bash_task = bash()

        assert bash_task.operator.task_id == "bash"
        assert bash_task.operator.bash_command == NOTSET
        assert bash_task.operator.env is None
        assert bash_task.operator.append_env is False
        assert bash_task.operator.output_encoding == "utf-8"
        assert bash_task.operator.skip_on_exit_code == [99]
        assert bash_task.operator.cwd is None
        assert bash_task.operator._init_bash_command_not_set is True

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.parametrize(
        argnames=["command", "expected_command", "expected_return_val"],
        argvalues=[
            pytest.param("echo hello world", "echo hello world", "hello world", id="not_templated"),
            pytest.param(
                "echo {{ ds }}", f"echo {DEFAULT_DATE.date()}", str(DEFAULT_DATE.date()), id="templated"
            ),
        ],
    )
    def test_bash_command(self, command, expected_command, expected_return_val):
        """Test the runtime bash_command is the function's return string, rendered if needed."""

        with self.dag:

            @task.bash
            def bash():
                return command

            bash_task = bash()

        assert bash_task.operator.bash_command == NOTSET

        ti, return_val = self.execute_task(bash_task)

        assert bash_task.operator.bash_command == expected_command
        assert return_val == expected_return_val

        self.validate_bash_command_rtif(ti, expected_command)

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_op_args_kwargs(self):
        """Test op_args and op_kwargs are passed to the bash_command."""

        with self.dag:

            @task.bash
            def bash(id, other_id):
                return f"echo hello {id} && echo {other_id}"

            bash_task = bash("world", other_id="2")

        assert bash_task.operator.bash_command == NOTSET

        ti, return_val = self.execute_task(bash_task)

        assert bash_task.operator.bash_command == "echo hello world && echo 2"
        assert return_val == "2"

        self.validate_bash_command_rtif(ti, "echo hello world && echo 2")

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_multiline_command(self):
        """Verify a multi-line string can be used as a Bash command."""
        command = """
        echo {foo} |
        rev
        """
        excepted_command = command.format(foo="foo")

        with self.dag:

            @task.bash
            def bash(foo):
                return command.format(foo=foo)

            bash_task = bash("foo")

        assert bash_task.operator.bash_command == NOTSET

        ti, return_val = self.execute_task(bash_task)

        assert return_val == "oof"
        assert bash_task.operator.bash_command == excepted_command

        self.validate_bash_command_rtif(ti, excepted_command)

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.parametrize(
        argnames=["append_env", "user_defined_env", "expected_airflow_home"],
        argvalues=[
            pytest.param(False, {"var": "value"}, "", id="no_append_env"),
            pytest.param(True, {"var": "value"}, "path/to/airflow/home", id="append_env"),
        ],
    )
    def test_env_variables(self, append_env, user_defined_env, expected_airflow_home, caplog):
        """Test env variables exist appropriately depending on if the existing env variables are allowed."""
        with self.dag:

            @task.bash(env=user_defined_env, append_env=append_env)
            def bash():
                return "echo var=$var; echo AIRFLOW_HOME=$AIRFLOW_HOME;"

            bash_task = bash()

        assert bash_task.operator.bash_command == NOTSET

        with mock.patch.dict("os.environ", {"AIRFLOW_HOME": "path/to/airflow/home"}):
            ti, return_val = self.execute_task(bash_task)

        assert bash_task.operator.env == user_defined_env
        assert "var=value" in caplog.text
        assert f"AIRFLOW_HOME={expected_airflow_home}" in caplog.text

        self.validate_bash_command_rtif(ti, "echo var=$var; echo AIRFLOW_HOME=$AIRFLOW_HOME;")

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.parametrize(
        argnames=["exit_code", "expected"],
        argvalues=[
            pytest.param(99, pytest.raises(AirflowSkipException), id="skip"),
            pytest.param(1, pytest.raises(AirflowException), id="non-zero"),
            pytest.param(0, no_raise(), id="zero"),
        ],
    )
    def test_exit_code_behavior(self, exit_code, expected):
        """Test @task.bash tasks behave appropriately relative the exit code from the bash_command."""

        with self.dag:

            @task.bash
            def bash(code):
                return f"exit {code}"

            bash_task = bash(exit_code)

        assert bash_task.operator.bash_command == NOTSET

        with expected:
            ti, return_val = self.execute_task(bash_task)

            self.validate_bash_command_rtif(ti, f"exit {exit_code}")

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.parametrize(
        argnames=["skip_on_exit_code", "exit_code", "expected"],
        argvalues=[
            pytest.param(None, 99, pytest.raises(AirflowSkipException), id="default_skip_exit_99"),
            pytest.param(None, 1, pytest.raises(AirflowException), id="default_skip_exit_1"),
            pytest.param(None, 0, no_raise(), id="default_skip_exit_0"),
            pytest.param({"skip_on_exit_code": 86}, 0, no_raise(), id="skip_86_exit_0"),
            pytest.param(
                {"skip_on_exit_code": 100}, 42, pytest.raises(AirflowException), id="skip_100_exit_42"
            ),
            pytest.param(
                {"skip_on_exit_code": 100}, 100, pytest.raises(AirflowSkipException), id="skip_100_exit_100"
            ),
            pytest.param(
                {"skip_on_exit_code": [100, 101]},
                100,
                pytest.raises(AirflowSkipException),
                id="skip_100-101_exit_100",
            ),
            pytest.param(
                {"skip_on_exit_code": [100, 101]},
                102,
                pytest.raises(AirflowException),
                id="skip_100-101_exit_102",
            ),
        ],
    )
    def test_skip_on_exit_code_behavior(self, skip_on_exit_code, exit_code, expected):
        """Ensure tasks behave appropriately relative to defined skip exit code from the bash_command."""

        with self.dag:

            @task.bash(**skip_on_exit_code if skip_on_exit_code else {})
            def bash(code):
                return f"exit {code}"

            bash_task = bash(exit_code)

        assert bash_task.operator.bash_command == NOTSET

        with expected:
            ti, return_val = self.execute_task(bash_task)

            self.validate_bash_command_rtif(ti, f"exit {exit_code}")

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.parametrize(
        argnames=[
            "user_defined_env",
            "append_env",
            "expected_razz",
            "expected_airflow_home",
        ],
        argvalues=[
            pytest.param(
                {"razz": "matazz"}, True, "matazz", "path/to/airflow/home", id="user_defined_env_and_append"
            ),
            pytest.param({"razz": "matazz"}, False, "matazz", "", id="user_defined_env_no_append"),
            pytest.param({}, True, "", "path/to/airflow/home", id="no_user_defined_env_and_append"),
            pytest.param({}, False, "", "", id="no_user_defined_env_no_append"),
        ],
    )
    def test_env_variables_in_bash_command_file(
        self,
        user_defined_env,
        append_env,
        expected_razz,
        expected_airflow_home,
        tmp_path,
        caplog,
    ):
        """Test the behavior of user-defined env vars when using an external file with a Bash command."""
        cmd_file = tmp_path / "test_file.sh"
        cmd_file.write_text("#!/usr/bin/env bash\necho AIRFLOW_HOME=$AIRFLOW_HOME\necho razz=$razz\n")
        cmd_file.chmod(stat.S_IEXEC)

        with self.dag:

            @task.bash(env=user_defined_env, append_env=append_env)
            def bash(command_file_name):
                return command_file_name

            with mock.patch.dict("os.environ", {"AIRFLOW_HOME": "path/to/airflow/home"}):
                bash_task = bash(f"{cmd_file} ")

                assert bash_task.operator.bash_command == NOTSET

                ti, return_val = self.execute_task(bash_task)

        assert f"razz={expected_razz}" in caplog.text
        assert f"AIRFLOW_HOME={expected_airflow_home}" in caplog.text
        assert return_val == f"razz={expected_razz}"
        self.validate_bash_command_rtif(ti, f"{cmd_file} ")

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_valid_cwd(self, tmp_path):
        """Test a user-defined working directory can be used."""
        cwd_path = tmp_path / "test_cwd"
        cwd_path.mkdir()

        with self.dag:

            @task.bash(cwd=os.fspath(cwd_path))
            def bash():
                return "echo foo | tee output.txt"

            bash_task = bash()

        assert bash_task.operator.bash_command == NOTSET

        ti, return_val = self.execute_task(bash_task)

        assert return_val == "foo"
        assert (cwd_path / "output.txt").read_text().splitlines()[0] == "foo"
        self.validate_bash_command_rtif(ti, "echo foo | tee output.txt")

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_cwd_does_not_exist(self, tmp_path):
        """Verify task failure for non-existent, user-defined working directory."""
        cwd_path = tmp_path / "test_cwd"

        with self.dag:

            @task.bash(cwd=os.fspath(cwd_path))
            def bash():
                return "echo"

            bash_task = bash()

        assert bash_task.operator.bash_command == NOTSET

        dr = self.dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        with pytest.raises(AirflowException, match=f"Can not find the cwd: {cwd_path}"):
            ti.run()
        assert ti.task.bash_command == "echo"

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_cwd_is_file(self, tmp_path):
        """Verify task failure for user-defined working directory that is actually a file."""
        cwd_file = tmp_path / "testfile.var.env"
        cwd_file.touch()

        with self.dag:

            @task.bash(cwd=os.fspath(cwd_file))
            def bash():
                return "echo"

            bash_task = bash()

        assert bash_task.operator.bash_command == NOTSET

        dr = self.dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        with pytest.raises(AirflowException, match=f"The cwd {cwd_file} must be a directory"):
            ti.run()
        assert ti.task.bash_command == "echo"

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_command_not_found(self):
        """Fail task if executed command is not found on path."""

        with self.dag:

            @task.bash
            def bash():
                return "set -e; something-that-isnt-on-path"

            bash_task = bash()

        assert bash_task.operator.bash_command == NOTSET

        dr = self.dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        with pytest.raises(
            AirflowException, match="Bash command failed\\. The command returned a non-zero exit code 127\\."
        ):
            ti.run()
        assert ti.task.bash_command == "set -e; something-that-isnt-on-path"

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_multiple_outputs_true(self):
        """Verify setting `multiple_outputs` for a @task.bash-decorated function is ignored."""

        with self.dag:

            @task.bash(multiple_outputs=True)
            def bash():
                return "echo"

            with pytest.warns(
                UserWarning, match="`multiple_outputs=True` is not supported in @task.bash tasks. Ignoring."
            ):
                bash_task = bash()

                assert bash_task.operator.bash_command == NOTSET

                ti, _ = self.execute_task(bash_task)

        assert bash_task.operator.multiple_outputs is False
        self.validate_bash_command_rtif(ti, "echo")

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.parametrize(
        "multiple_outputs", [False, pytest.param(None, id="none"), pytest.param(NOTSET, id="not-set")]
    )
    def test_multiple_outputs(self, multiple_outputs):
        """Verify setting `multiple_outputs` for a @task.bash-decorated function is ignored."""

        decorator_kwargs = {}
        if multiple_outputs is not NOTSET:
            decorator_kwargs["multiple_outputs"] = multiple_outputs

        with self.dag:

            @task.bash(**decorator_kwargs)
            def bash():
                return "echo"

            with warnings.catch_warnings():
                warnings.simplefilter("error", category=UserWarning)

                bash_task = bash()

                assert bash_task.operator.bash_command == NOTSET

                ti, _ = self.execute_task(bash_task)

        assert bash_task.operator.multiple_outputs is False
        self.validate_bash_command_rtif(ti, "echo")

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.parametrize(
        argnames=["return_val", "expected"],
        argvalues=[
            pytest.param(None, pytest.raises(TypeError), id="return_none_typeerror"),
            pytest.param(1, pytest.raises(TypeError), id="return_int_typeerror"),
            pytest.param(NOTSET, pytest.raises(TypeError), id="return_notset_typeerror"),
            pytest.param(True, pytest.raises(TypeError), id="return_boolean_typeerror"),
            pytest.param("", pytest.raises(TypeError), id="return_empty_string_typerror"),
            pytest.param("  ", pytest.raises(TypeError), id="return_spaces_string_typerror"),
            pytest.param(["echo;", "exit 99;"], pytest.raises(TypeError), id="return_list_typerror"),
            pytest.param("echo", no_raise(), id="return_string_no_error"),
        ],
    )
    def test_callable_return_is_string(self, return_val, expected):
        """Ensure the returned value from the decorated callable is a non-empty string."""

        with self.dag:

            @task.bash
            def bash():
                return return_val

            bash_task = bash()

        assert bash_task.operator.bash_command == NOTSET

        with expected:
            ti, _ = self.execute_task(bash_task)

            self.validate_bash_command_rtif(ti, return_val)

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_rtif_updates_upon_failure(self):
        """Veriy RenderedTaskInstanceField data should contain the rendered command even if the task fails."""
        with self.dag:

            @task.bash
            def bash():
                return "{{ ds }}; exit 1;"

            bash_task = bash()

        assert bash_task.operator.bash_command == NOTSET

        dr = self.dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        with pytest.raises(AirflowException):
            ti.run()
        assert ti.task.bash_command == f"{DEFAULT_DATE.date()}; exit 1;"

    def test_templated_bash_script(self, dag_maker, tmp_path, session):
        """
        Creates a .sh script with Jinja template.
        Pass it to the BashOperator and ensure it gets correctly rendered and executed.
        """
        bash_script: str = "sample.sh"
        path: Path = tmp_path / bash_script
        path.write_text('echo "{{ ti.task_id }}"')

        with dag_maker(
            dag_id="test_templated_bash_script", session=session, template_searchpath=os.fspath(path.parent)
        ):

            @task.bash
            def test_templated_fields_task():
                return bash_script

            test_templated_fields_task()

        ti: TaskInstance = dag_maker.create_dagrun().task_instances[0]
        session.add(ti)
        session.commit()
        context = ti.get_template_context(session=session)
        ti.render_templates(context=context)

        op: BashOperator = ti.task
        result = op.execute(context=context)
        assert result == "test_templated_fields_task"
