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

from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowSkipException,
    AirflowTaskTimeout,
    timezone,
)
from airflow.providers.standard.hooks.subprocess import SubprocessResult
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

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

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("append_env", "user_defined_env", "expected_airflow_home"),
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
        date_env_name = "$AIRFLOW_CTX_LOGICAL_DATE" if AIRFLOW_V_3_0_PLUS else "$AIRFLOW_CTX_EXECUTION_DATE"
        with dag_maker(
            "bash_op_test",
            default_args={"owner": "airflow", "retries": 100, "start_date": DEFAULT_DATE},
            schedule="@daily",
            dagrun_timeout=timedelta(minutes=60),
            serialized=True,
        ):
            tmp_file = tmp_path / "testfile"
            BashOperator(
                task_id="echo_env_vars",
                bash_command=f"echo $AIRFLOW_HOME>> {tmp_file};"
                f"echo $PYTHONPATH>> {tmp_file};"
                f"echo $AIRFLOW_CTX_DAG_ID >> {tmp_file};"
                f"echo $AIRFLOW_CTX_TASK_ID>> {tmp_file};"
                f"echo {date_env_name}>> {tmp_file};"
                f"echo $AIRFLOW_CTX_DAG_RUN_ID>> {tmp_file};",
                append_env=append_env,
                env=user_defined_env,
            )

        logical_date = utc_now
        dr = dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
            start_date=utc_now,
            state=State.RUNNING,
            data_interval=(logical_date, logical_date),
        )

        with mock.patch.dict(
            "os.environ", {"AIRFLOW_HOME": "MY_PATH_TO_AIRFLOW_HOME", "PYTHONPATH": "AWESOME_PYTHONPATH"}
        ):
            dag_maker.run_ti("echo_env_vars", dr)

        assert expected == tmp_file.read_text()

    @pytest.mark.parametrize(
        ("val", "expected"),
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
        ("extra_kwargs", "actual_exit_code", "expected_exc"),
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
            BashOperator(
                task_id="test_bash_operator_kill",
                execution_timeout=timedelta(microseconds=25),
                bash_command=f"/bin/bash -c 'sleep {sleep_time}'",
            )
        dr = dag_maker.create_dagrun()
        with pytest.raises(AirflowTaskTimeout):
            dag_maker.run_ti("test_bash_operator_kill", dr)
        sleep(2)
        for proc in psutil.process_iter():
            if proc.cmdline() == ["sleep", sleep_time]:
                os.kill(proc.pid, signal.SIGTERM)
                pytest.fail("BashOperator's subprocess still running after stopping on timeout!")

    @pytest.mark.db_test
    def test_templated_fields(self, dag_maker, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            BashOperator,
            # Templated fields
            bash_command='echo "{{ dag_run.dag_id }}"',
            env={"FOO": "{{ ds }}"},
            cwd="{{ task.dag.folder }}",
            # Other parameters
            dag_id="test_templated_fields_dag",
            task_id="test_templated_fields_task",
        )
        context = {
            "dag": dag_maker.dag,
            "dag_run": ti.dag_run,
            "ds": "~whatever~",
            "task": dag_maker.dag.get_task(ti.task_id),
            "ti": ti,
        }
        task = ti.render_templates(context=context)
        assert task.bash_command == 'echo "test_templated_fields_dag"'
        assert task.cwd == Path(__file__).absolute().parent.as_posix()

    @mock.patch.object(BashOperator, "_run_inline_command")
    @mock.patch.object(
        BashOperator, "_run_rendered_script_file", return_value=SubprocessResult(exit_code=0, output="ok")
    )
    def test_execute_detects_script_after_bash_command_is_rendered(
        self, mock_run_rendered_script_file, mock_run_inline_command, context
    ):
        op = BashOperator(task_id="abc", bash_command="{{ bash_script }}")
        op.bash_command = "sample.sh"

        result = op.execute(context)

        assert result == "ok"
        mock_run_rendered_script_file.assert_called_once()
        mock_run_inline_command.assert_not_called()

    @pytest.mark.db_test
    def test_templated_bash_script(self, dag_maker, create_task_instance_of_operator, tmp_path, session):
        """
        Creates a .sh script with Jinja template.
        Pass it to the BashOperator and ensure it gets correctly rendered and executed.
        """
        bash_script: str = "sample.sh"
        path: Path = tmp_path / bash_script
        path.write_text('echo "{{ ti.task_id }}"')

        ti = create_task_instance_of_operator(
            BashOperator,
            dag_id="test_templated_bash_script",
            template_searchpath=os.fspath(path.parent),
            task_id="test_templated_fields_task",
            bash_command=bash_script,
        )
        context = {"dag": dag_maker.dag, "ti": ti}
        task = ti.render_templates(context=context)
        result = task.execute(context=context)
        assert result == "test_templated_fields_task"


def _pushed_xcoms(context) -> dict[str, object]:
    """Turn ``context["ti"].xcom_push`` calls into a plain ``{key: value}`` dict."""
    return {call.kwargs["key"]: call.kwargs["value"] for call in context["ti"].xcom_push.call_args_list}


def _execute(op: BashOperator, context, exit_code: int) -> None:
    """Run ``op`` and check it ends the way ``exit_code`` alone dictates."""
    if exit_code == 0:
        op.execute(context)
    else:
        with pytest.raises(AirflowSkipException if exit_code == 99 else AirflowException) as exc_info:
            op.execute(context)
        assert f"exit code {exit_code}" in str(exc_info.value)


class TestBashOperatorXComDir:
    def test_plain_file_strips_one_trailing_newline(self, context):
        op = BashOperator(task_id="abc", bash_command="printf 'a\\n\\n' > \"$AIRFLOW_XCOM_DIR/k\"")
        op.execute(context)
        assert _pushed_xcoms(context) == {"k": "a\n"}

    @pytest.mark.parametrize(
        ("json_payload", "expected"),
        [
            ('{"a": 1}', {"a": 1}),
            ("[1, 2, 3]", [1, 2, 3]),
            ("42", 42),
        ],
    )
    def test_json_file_is_parsed_under_stem_key(self, json_payload, expected, context):
        op = BashOperator(
            task_id="abc",
            bash_command=f"printf '{json_payload}' > \"$AIRFLOW_XCOM_DIR/d.json\"",
        )
        op.execute(context)
        assert _pushed_xcoms(context) == {"d": expected}

    def test_value_with_quotes_and_newlines_survives_verbatim(self, context):
        op = BashOperator(
            task_id="abc",
            bash_command=(
                "cat <<'XCOM_EOF' > \"$AIRFLOW_XCOM_DIR/k\"\n"
                "line one\n"
                "line two with 'single' and \"double\" quotes\n"
                "last line\n"
                "XCOM_EOF\n"
            ),
        )
        op.execute(context)
        assert _pushed_xcoms(context) == {
            "k": "line one\nline two with 'single' and \"double\" quotes\nlast line"
        }

    @pytest.mark.parametrize("exit_code", [7, 99])
    def test_entries_are_pushed_when_the_command_fails_or_skips(self, exit_code, context):
        op = BashOperator(task_id="abc", bash_command=f'printf x > "$AIRFLOW_XCOM_DIR/k"; exit {exit_code}')
        _execute(op, context, exit_code)
        assert _pushed_xcoms(context) == {"k": "x"}

    @pytest.mark.parametrize("exit_code", [0, 3, 99])
    @pytest.mark.parametrize(
        ("write_bad_entry", "entry_name"),
        [
            pytest.param('mkdir "$AIRFLOW_XCOM_DIR/tmp"', "tmp", id="subdirectory"),
            pytest.param('ln -s good "$AIRFLOW_XCOM_DIR/link"', "link", id="symlink"),
            pytest.param('printf abcdefgh > "$AIRFLOW_XCOM_DIR/big"', "big", id="too-large"),
            pytest.param("printf '\\377' > \"$AIRFLOW_XCOM_DIR/binary\"", "binary", id="undecodable"),
            pytest.param('printf v > "$AIRFLOW_XCOM_DIR/a?b"', "a?b", id="unsafe-name"),
            pytest.param('printf v > "$AIRFLOW_XCOM_DIR/return_value"', "return_value", id="return-value"),
            pytest.param(
                'printf "{}" > "$AIRFLOW_XCOM_DIR/return_value.json"',
                "return_value.json",
                id="return-value-json",
            ),
        ],
    )
    def test_bad_entry_is_skipped_without_changing_the_outcome(
        self, write_bad_entry, entry_name, exit_code, context, caplog
    ):
        op = BashOperator(
            task_id="abc",
            bash_command=f'{write_bad_entry}; printf v > "$AIRFLOW_XCOM_DIR/good"; exit {exit_code}',
            max_xcom_file_size=5,
        )
        _execute(op, context, exit_code)
        assert _pushed_xcoms(context) == {"good": "v"}
        assert any(m.startswith(f"Skipped XCom directory entry {entry_name!r}: ") for m in caplog.messages)

    @pytest.mark.parametrize("exit_code", [0, 3, 99])
    def test_malformed_json_is_pushed_raw_under_its_full_name(self, exit_code, context, caplog):
        op = BashOperator(
            task_id="abc",
            bash_command=f"printf '{{not json' > \"$AIRFLOW_XCOM_DIR/config.json\"; exit {exit_code}",
        )
        _execute(op, context, exit_code)
        assert _pushed_xcoms(context) == {"config.json": "{not json"}
        assert any(m.startswith("XCom file 'config.json' contains invalid JSON") for m in caplog.messages)

    @pytest.mark.parametrize("exit_code", [0, 3, 99])
    def test_unreadable_directory_does_not_change_the_outcome(self, exit_code, context, caplog):
        op = BashOperator(task_id="abc", bash_command=f'rm -rf "$AIRFLOW_XCOM_DIR"; exit {exit_code}')
        _execute(op, context, exit_code)
        context["ti"].xcom_push.assert_not_called()
        assert any(m.startswith("Could not read the XCom directory: ") for m in caplog.messages)

    def test_dot_entries_are_ignored_silently(self, context, caplog):
        op = BashOperator(
            task_id="abc",
            bash_command=(
                'mkdir "$AIRFLOW_XCOM_DIR/.scratch"; printf v > "$AIRFLOW_XCOM_DIR/.partial"; '
                'printf v > "$AIRFLOW_XCOM_DIR/k"'
            ),
        )
        op.execute(context)
        assert _pushed_xcoms(context) == {"k": "v"}
        assert not any(m.startswith("Skipped XCom directory entry") for m in caplog.messages)

    def test_return_value_file_does_not_replace_the_return_value(self, context):
        op = BashOperator(
            task_id="abc",
            bash_command='printf rv > "$AIRFLOW_XCOM_DIR/return_value"; echo "last line"',
            output_processor=str.upper,
        )
        assert op.execute(context) == "LAST LINE"
        assert _pushed_xcoms(context) == {}

    def test_do_xcom_push_false_disables_the_mechanism(self, context):
        op = BashOperator(
            task_id="abc",
            bash_command='[ -z "$AIRFLOW_XCOM_DIR" ] && exit 0 || exit 1',
            do_xcom_push=False,
        )
        op.execute(context)
        context["ti"].xcom_push.assert_not_called()

    def test_execute_does_not_mutate_the_operators_env(self, context):
        user_env = {"var": "value"}
        op = BashOperator(
            task_id="abc",
            bash_command='echo "$var" > "$AIRFLOW_XCOM_DIR/seen"',
            env=user_env,
            append_env=False,
        )
        op.execute(context)

        assert _pushed_xcoms(context) == {"seen": "value"}
        assert "AIRFLOW_XCOM_DIR" not in op.env
        assert user_env["var"] == "value"
